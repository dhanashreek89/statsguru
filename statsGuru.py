#!/usr/bin/python

import collections, random, subprocess, re, sys, getopt

def getReltuples(database, schema, table):
    #TODO: Normalize the statistic data for reltuples > 100000
    reltuplesCommand = "select reltuples from pg_class where relname = '{}' and relnamespace = (select oid from pg_namespace where nspname = '{}' and reltuples <= 100000);".format(table, schema)
    sampleCommand = "psql -d {} -c \"{}\"".format(database, reltuplesCommand)
    result = subprocess.check_output(sampleCommand, shell=True)
    result = result.split('\n')
    return int(result[2].lstrip())

def getStats(database, schema, table):
    statsCommand = "select attname, null_frac, n_distinct, most_common_vals, most_common_freqs, histogram_bounds from pg_stats where schemaname = '{}' and tablename = '{}' ".format(schema, table)
    sampleCommand = "psql -d {} -c \"{}\"".format(database, statsCommand)
    result = subprocess.check_output(sampleCommand, shell=True)

    lines = result.split('\n')
    i=0
    attname = []
    stats_tuple = collections.OrderedDict()
    
    for l in lines:
        i = i + 1
        if i <= 2 or l.strip() == '' or re.match('\(\d rows\)', l): 
            continue
        row = l.split('|')
        stats_tuple[row[0].rstrip().lstrip()] = row[1:]
    return stats_tuple

def getMcvData(mcv_col, mcf_col, reltuples):
    mcv_data = {}
    if mcv_col.find('{') == -1:
        return mcv_data
    mcv = (((mcv_col.rstrip().lstrip()).translate(None, '{')).translate(None, '}')).split(',')
    mcf = (((mcf_col.rstrip().lstrip()).translate(None, '{')).translate(None, '}')).split(',')
    for val, freq in zip(mcv, mcf):
        mcv_data[val] = int(round((float(freq) * reltuples), 0))

    return mcv_data

def getNDVRem(ndv, mcv_data,reltuples):
    ndvF = float(ndv.rstrip().lstrip())
    if ndvF < 0.0:
        return int(round((-1 * (ndvF * reltuples)),0)) - len(mcv_data)
    elif int(ndvF) == -1:
        return reltuples
    else:
        return int(round(ndvF)) - len(mcv_data)

def getBucketSize(reltuples, nullCount, mcv_data, num_buckets):
    remTuples = reltuples - nullCount
    for val in mcv_data:
        remTuples = remTuples - mcv_data[val]
    return remTuples/num_buckets

def collectDataFromStats(database, schema, table):
    reltuples =  getReltuples(database, schema, table)
    stats_tuple = getStats(database, schema, table)

    data_info = collections.OrderedDict()
    for attr in stats_tuple:
        attr = attr.rstrip().lstrip()
        row = stats_tuple[attr]
        nullCount = int(round((float(row[0].lstrip()) * reltuples), 0))
        mcv_data = getMcvData(row[2], row[3], reltuples)
        ndvRem = getNDVRem(row[1], mcv_data, reltuples)
        histogram_bound = (((row[4].rstrip().lstrip()).translate(None, '{')).translate(None, '}')).split(',')
        bucketSize = getBucketSize(reltuples, nullCount, mcv_data, len(histogram_bound)-1)
        ndvPerBucket = ndvRem / (len(histogram_bound)-1)

        data_info[attr] = []
        data_info[attr].append(reltuples)
        data_info[attr].append(nullCount)
        data_info[attr].append(mcv_data)
        data_info[attr].append(histogram_bound)
        data_info[attr].append(bucketSize)
        data_info[attr].append(ndvRem)
        data_info[attr].append(ndvPerBucket)

    return data_info

def joinTables(attributes, database, schema, table):
    projection = "select "
    fromClause = " from "
    whereClause = " where "
    i = 1
    for attr in attributes:
        projection = projection + "f" + str(i) + "." + attr + ","
        fromClause = fromClause + "(select row_number() over() as num, {} from temp{}) f{}".format(attr, attr, i) + ", "
        if i > 1:
            whereClause = whereClause + "f{}.num = f{}.num AND ".format(i-1, i)
        i = i +1
    projection = projection[:len(projection)-1]
    fromClause = fromClause[:len(fromClause)-2]
    whereClause = whereClause[:len(whereClause)-5]

    finalCmd = "psql -d {} -c \"Insert into {}.{} {} {} {}\"".format(database,schema, table, projection, fromClause, whereClause)
    subprocess.check_output(finalCmd, shell=True)

def createData(data_info, database, schema, table):
    for attr in data_info:
        createTableCmd = "psql -d {} -c \"create table temp{}({} int)\"".format(database, attr, attr)
        subprocess.check_output(createTableCmd, shell=True)

        ## generate null for this column
        val_string = ''
        for i in range(0,(data_info[attr])[1]):
            val_string = val_string + '(NULL),'
        if val_string != '':
            val_string = val_string[:len(val_string)-1]
            insertNullCmd = "psql -d {} -c \"insert into temp{} values{}\"".format(database, attr, val_string)
            subprocess.check_output(insertNullCmd, shell=True)

        ## MCV inserts
        val_string = ''
        for val in (data_info[attr])[2]:
            for i in range(0,data_info[attr][2][val]):
                val_string = val_string + "(" + str(val) + ")" + ","
        if val_string != '':
            val_string = val_string[:len(val_string)-1]
            insertMcvCmd = "psql -d {} -c \"insert into temp{} values{}\"".format(database, attr, val_string)
            subprocess.check_output(insertMcvCmd, shell=True)

        ## Histogram inserts
        startbound = data_info[attr][3][0]
        bucketSize = data_info[attr][4]
        val_string = ''
        ndvPerBucket = data_info[attr][6]
        for endbound in (data_info[attr])[3][1:]:
            vals = set()
            while len(vals) < ndvPerBucket and len(vals) < (int(endbound) - int(startbound)):
                for i in range(0,bucketSize):
                    val = random.randint(int(startbound), int(endbound))
                    vals.add(val)
                vals = vals.difference(set(data_info[attr][2].keys()))
            for j in range(0,len(vals)):
                val = vals.pop()
                for i in range(0, bucketSize / min(ndvPerBucket, (int(endbound) - int(startbound) ))):
                    val_string = val_string + "(" + str(val) + ")" + "," 
            startbound = endbound
    
        if val_string != '':
            val_string = val_string[:len(val_string)-1]
            insertHistCmd =  "psql -d {} -c \"insert into temp{} values {}\"".format(database, attr, val_string)
            subprocess.check_output(insertHistCmd, shell=True)

    joinTables(data_info.keys(), database, schema, table)

    for attr in data_info:
        dropTableCmd = "psql -d {} -c \"drop table temp{}\"".format(database, attr)
        subprocess.check_output(dropTableCmd, shell=True)

def main(argv):
    database = ''
    schema = ''
    table = ''
    try:
        opts, args = getopt.getopt(argv,"hd:s:t:",["database=","schema=", "table="])
    except getopt.GetoptError:
        print 'test.py -d <database> -s <schema> -t <table>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'test.py -d <database> -s <schema> -t <table>'
            sys.exit()
        elif opt in ("-d", "--database"):
            database = arg
        elif opt in ("-s", "--schema"):
            schema = arg
        elif opt in ("-t", "--table"):
            table = arg	

    data_info = collectDataFromStats(database, schema, table)
    createData(data_info, database, schema, table)


#TODO: use default datasbe and/or schema name when not specified. Also error out if no table name is provided

if __name__ == "__main__":
    main(sys.argv[1:])
