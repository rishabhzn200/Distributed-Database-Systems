#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    query = """CREATE TABLE {tablename}(USERID INT, MOVIEID INT, RATING NUMERIC NOT NULL CHECK(RATING>=0 AND RATING<=5));"""
    # Create ratings table
    cur.execute(query.format(tablename=ratingstablename))

    with open(ratingsfilepath, 'r') as ratingfileobj:
        while True:
            ratingfiledata = ratingfileobj.readline()
            if not ratingfiledata:
                break

            user, movie, rating, timestamp = ratingfiledata.strip().split('::')
            # print str(user)+ "\t" + str(movie)+ "\t"  + str(rating)+ "\t" + str(timestamp) + "\n" 
            query = """insert into ratings VALUES ({uid}, {movieid}, {rating});""".format(uid=user, movieid=movie, rating=rating)
            cur.execute(query)

    pass



def metadata_range(cur, num, tablename='range_metadata'):

    # Drop the old table, create new table, add value to it

    q_drop = """drop table {tablename};""".format(tablename=tablename)

    q_create = """CREATE TABLE {tablename}(num INT);""".format(tablename=tablename)
    q_insert = """insert into {tablename} values({num})""".format(tablename=tablename, num=num)

    # Drop table if already present
    try:
        cur.execute(q_drop)
    except:
        pass
    cur.execute(q_create)
    cur.execute(q_insert)

def getnumfrom_metadata_range(cur, tablename='range_metadata'):
    q_select = """select num from {tablename}""".format(tablename=tablename)
    cur.execute(q_select)
    return cur.fetchone()


def metadata_robin(cur, next_table_id_to_insert, numberofpartitions , tablename='robin_metadata'):
    # Drop the old table, create new table, add value to it

    q_drop = """drop table {tablename};""".format(tablename=tablename)

    q_create = """CREATE TABLE {tablename}(next_id INT, num INT);""".format(tablename=tablename)
    q_insert = """insert into {tablename} values({next_id},{num})""".format(tablename=tablename, next_id=next_table_id_to_insert, num=numberofpartitions)

    try:
        cur.execute(q_drop)
    except:
        pass
    cur.execute(q_create)
    cur.execute(q_insert)


def getnumfrom_metadata_robin(cur, tablename='robin_metadata'):
    q_select = """select next_id, num from {tablename}""".format(tablename=tablename)
    cur.execute(q_select)
    return cur.fetchone()


def get_rating_partitions(numberofpartitions):
    mul_factor = 5.0/numberofpartitions
    return list(map(lambda x: (x * mul_factor, (x+1) * mul_factor), range(0, numberofpartitions)))

def get_interval(currrating, numpartitions):
    all_intervals = get_rating_partitions(numpartitions)

    # if currrating is 0. return 0
    if currrating == 0 or currrating == 0.0:
        return 0

    else:
        for index, interval in enumerate(all_intervals):
            if currrating > interval[0] and currrating <= interval[1]:
                return index


def createnewdatabase(cur, query, part_no):
    cur.execute(query.format(tablename=part_no))

def selectfromdatabase(cur, query_select, equalorspace,  ratingstablename, x):

    # patching: rating 0 to 1 in 0th partition, >1 to 2 in 1st partition etc
    if equalorspace == 0:
        cur.execute(query_select.format(tablename=ratingstablename, equalorspace='=', start_val=x[0], end_val=x[1]))

    else:
        cur.execute(query_select.format(tablename=ratingstablename, equalorspace='', start_val=x[0], end_val=x[1]))
    return cur.fetchall()

def inserintodatabase(cur, query_insert, tablename, row):
    # Unpack the row
    user, movie, rating = row
    cur.execute(query_insert.format(tablename=tablename, uid=user, movieid=movie, rating=rating))


def fetch_and_add(ratingstablename, rating_partition_intervals, openconnection, partnameprefix):
    # Get the cursor
    cur = openconnection.cursor()
    query_create = """CREATE TABLE {tablename}(USERID INT, MOVIEID INT, RATING NUMERIC NOT NULL CHECK(RATING>=0 AND RATING<=5));"""

    query_select = """select * from {tablename} where rating>{equalorspace}{start_val} and rating<={end_val}"""

    query_insert = """insert into {tablename} VALUES ({uid}, {movieid}, {rating})"""

    # Create databases - partitioned
    map(lambda part_no: createnewdatabase(cur, query_create, partnameprefix +str(part_no)) , range(len(rating_partition_intervals)))

    # Get the rows in the interval
    rating_interval_rows = list(map(lambda x: selectfromdatabase(cur, query_select, x[0], ratingstablename, x[1]), enumerate(rating_partition_intervals)  ))

    # for i, r in enumerate(rating_interval_rows):
    #     print "Table_{0} = {1}".format(str(i), str(r))

    map(lambda index_rowlist: [inserintodatabase(cur, query_insert, partnameprefix+str(index_rowlist[0]), row) for row in index_rowlist[1] ], enumerate(rating_interval_rows))

    # map(lambda index_rowlist:  for row in range(len(index_rowlist[1])), enumerate(range(len(rating_interval_rows))) )

    # import sys
    # sys.exit()



def rangepartition(ratingstablename, numberofpartitions, openconnection):

    # Step 1: Get the cursor
    cur = openconnection.cursor()

    # Step 2: Store the numpartitions in metadata table
    metadata_range(cur, numberofpartitions, tablename='range_metadata')

    # currently importing library to inspect and get the partition prefix
    partnameprefix = ''
    try:
        import inspect
        partnameprefix = inspect.stack()[1][0].f_globals['RANGE_TABLE_PREFIX']
    except:
        partnameprefix = 'range_part'
    
    # print "ratingtablename = " + partnameprefix
    # print "Globals = " + str(inspect.stack()[1][0].f_globals)

    # Get the partition range
    rating_partition_intervals = get_rating_partitions(numberofpartitions)
    # print "Rating range is = " + str(rating_partition_intervals)


    # Fetch the results from the table and add to database
    fetch_and_add(ratingstablename, rating_partition_intervals, openconnection, partnameprefix)
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    # Step 1: Get the cursor
    cur = openconnection.cursor()

    

    

    partnameprefix = 'rrobin_part'


    # Step 4: Get all the values from db partition
    query_create = """CREATE TABLE {tablename}(USERID INT, MOVIEID INT, RATING NUMERIC NOT NULL CHECK(RATING>=0 AND RATING<=5));"""

    query_select = """select * from {tablename}"""

    query_insert = """insert into {tablename} VALUES ({uid}, {movieid}, {rating})"""

    # Create databases - partitioned
    map(lambda part_no: createnewdatabase(cur, query_create, partnameprefix +str(part_no)) , range(numberofpartitions))

    # Fetch all the value from db and insert
    # Get the rows in the interval
    # rating_interval_rows = list(map(lambda x: selectfromdatabase(cur, query_select, x[0], ratingstablename, x[1]), enumerate(rating_partition_intervals)  ))
    numrows = cur.execute(query_select.format(tablename=ratingstablename))
    
    next_table_id_to_insert = 0

    rowlists = cur.fetchall()
    for rowindex, row in enumerate(rowlists):
        part_no = rowindex % numberofpartitions
        next_table_id_to_insert = (part_no + 1) % numberofpartitions
        
        inserintodatabase(cur, query_insert, partnameprefix+str(part_no), row)


    # Step 2: Store in metadata table. May be at last
    metadata_robin(cur, next_table_id_to_insert, numberofpartitions , tablename='robin_metadata')





    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()
    curr_id, numpartition = getnumfrom_metadata_robin(cur, tablename='robin_metadata')
    row = (userid, itemid, rating)
    partnameprefix = 'rrobin_part'

    query_insert = """insert into {tablename} VALUES ({uid}, {movieid}, {rating})"""

    inserintodatabase(cur, query_insert, partnameprefix+str(curr_id), row)

    next_table_id_to_insert = (curr_id + 1) % numpartition

    # Update metadata
    # Step 2: Store in metadata table. May be at last
    metadata_robin(cur, next_table_id_to_insert, numpartition , tablename='robin_metadata')



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()
    # Question: numberofpartitions is not provided here?
    numpartitions, = getnumfrom_metadata_range(cur, tablename='range_metadata')
    # print "numpartitions = " + str(numpartitions)

    interval_id = get_interval(rating, numpartitions)
    partnameprefix = "range_part"
    row = (userid, itemid, rating)

    query_insert = """insert into {tablename} VALUES ({uid}, {movieid}, {rating})"""

    inserintodatabase(cur, query_insert, partnameprefix+str(interval_id), row)
    pass

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()