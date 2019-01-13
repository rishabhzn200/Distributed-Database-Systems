#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading
from Queue import Queue
# import heapq



def write_to_database(openconnection, input_table_name, output_table_name, schema, merged_data):
    cur = openconnection.cursor()

    # Small hack to remove the brackets from the string representation of a tuple
    data = str(tuple(merged_data))[1:-1]
    schema = ', '.join(schema)

    q_create = """create table {out_table} as table {in_table} with no data;""".format(out_table=output_table_name, in_table=input_table_name)
    q_insert = """insert into {out_table} ({schema}) values {data};""".format(out_table=output_table_name, schema=schema, data=data)

    cur.execute(q_create)
    cur.execute(q_insert)
    openconnection.commit()


def write_to_database_join(openconnection=None, output_table_name=None, schema=None, dtype_dict=None, joined_data=None):
    cur = openconnection.cursor()

    # Small hack to remove the brackets from the string representation of a tuple
    data = str(tuple(joined_data))[1:-1]

    # Hack to remove tblname.colname Remove
    schema = list(map(lambda x: x.split('.')[1], schema))
    combined_schema = ', '.join(schema)
    cq_schema = ' {dtype}, '.join(schema)

    cq_schema = list(map(lambda s : s + " " + dtype_dict[s],  schema))
    cq_schema = ', '.join(cq_schema)

    q_create = """create table {out_table} ({allschema});""".format(out_table=output_table_name, allschema=cq_schema)
    q_insert = """insert into {out_table} ({schema}) values {data};""".format(out_table=output_table_name, schema=combined_schema, data=data)

    cur.execute(q_create)
    cur.execute(q_insert)
    openconnection.commit()


def merge_data(numThreads, partially_sorted_data_list, schema, SortingColumnName):

    # Get the index of SortingColumnName
    sorting_col_index = schema.index(SortingColumnName.lower())
    num_pointers = numThreads
    
    # Get numThreads elements from all the list
    num_lists = len(partially_sorted_data_list)

    # This runs for numList times and initializes checklist
    check_min = []
    remove_list = []
    for psl_index, sorted_list in enumerate(partially_sorted_data_list):
        if len(sorted_list) == 0:
            remove_list.append(psl_index)
        else:
            # print(sorted_list)
            check_min.append(sorted_list.pop(0))

    # If any list is empty. Remove it
    if len(remove_list) != 0:
        for rm_index in remove_list:
            partially_sorted_data_list.pop(rm_index.pop())

    # Create the output list
    sorted_output_list = []

    # Above code runs 1 time and initializes check_min
    num_list_removed = 0
    initial_num_lists = len(partially_sorted_data_list)
    while len(partially_sorted_data_list) != 0:
        # check_min is initialized
        # Get the minimum element and its index from check_min
        min_value = min(check_min, key=lambda x: x[sorting_col_index])
        min_index = check_min.index(min_value)

        # print("Check_min = {cm}, min_index = {mn}".format(cm=check_min, mn=min_index))
        data_to_add_output = check_min.pop(min_index)
        # print("Data added to list = {data}".format(data=data_to_add_output))
        sorted_output_list.append(data_to_add_output)

        # Now we need to add the element from the same array as which had minimum. We have that value in min_index
        # Get the next value from that array and insert it at same position in the check_min
        # Before that check if the array is empty

        # print("********** min_index top =  {min_index}, {num_list_removed} *************".format(min_index=min_index, num_list_removed=num_list_removed))
        # print(len(partially_sorted_data_list))
        # print("Ending")

        # if len(partially_sorted_data_list) == 0:
        #     break

        if len(partially_sorted_data_list[min_index]) == 0:
            # print("********** min_index =  {min_index} *************".format(min_index=min_index))
            # If one of the array is empty then remove it from the lists
            partially_sorted_data_list.pop(min_index)
            num_list_removed += 1
        else:
            # List has the value. add the value to check_min
            check_min.insert(min_index, partially_sorted_data_list[min_index].pop(0))


    # check_min
    # Find minimum values from check_min and add to output lists
    while len(check_min) != 0:
        val = check_min.pop(check_min.index(min(check_min, key=lambda x: x[sorting_col_index])))
        sorted_output_list.append(val)

    return sorted_output_list



def get_data_from_queue(result_queue, get_type='append'):

    # Assuming result queue has None appended at the End to break form the while loop

    #
    # print("****** Result Queue Get **********")
    partiallySortedDataList = []

    while True:
        
        data = result_queue.get()
        if data is 'None':
            break
        if get_type == 'append':
            partiallySortedDataList.append(data[1])
        else:
            partiallySortedDataList.extend(data[1])
        # print('data = ', data[1])

    # print( "End")
    return partiallySortedDataList


def GetSplicingIndex(numThreads, numDataPoints):
    numIntervalRounded = numDataPoints // numThreads
    intervals = [(numIntervalRounded*index, numIntervalRounded* (index+1)) for index in range(numThreads-1)]
    # intervals.append('Hello')
    intervals.append((numIntervalRounded*(numThreads-1) , numDataPoints))
    # print(numIntervalRounded)
    # print(intervals)
    return intervals


def GetData(cur, InputTable):
    q_select = """select * from {tablename}"""
    select_query = q_select.format(tablename=InputTable)
    cur.execute(select_query)
    result = cur.fetchall()
    return result


def GetCount(cur, InputTable):
    q_count = """select count(*) from {tablename}"""
    count_query = q_count.format(tablename=InputTable)
    cur.execute(count_query)
    value = cur.fetchall()[0][0]
    # print("******")
    # print(value, type(value))
    # print("******")
    return value


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.

    # print('Entering Parallel Sort\n')

    # Create Worker Thread
    def worker_sort(conn=None, tablename=None, start_index=None, end_index=None, col=None):
        # Get the thread name
        threadName = threading.current_thread().name
        # print('Name : ', threadName, '\tStart : ', start_index, '\tEnd : ', end_index, '\tCol : ', col, '\n' )

        num_records = end_index - start_index
        tableoffset = start_index

        # Get the data from the table using the indexes
        cur = conn.cursor()
        with threading.Lock():
            
            q_select = """select * from (select * from {tablename} LIMIT {num_records} OFFSET {tableoffset} ) as temp ORDER BY {col}; """

            select_query = q_select.format(tablename=tablename, num_records=num_records, tableoffset=tableoffset, col=col)
            try:
                cur.execute(select_query)
            except:
                print('Execution Failed\n')

        # Get the table description
        table_description = cur.description
        table_description = list(map(lambda x: x.name.lower(), cur.description))
        result = cur.fetchall()
        # print(result[0], len(result))

        # Acquire the lock before putting the data to the Queue
        # print("Acquiring Lock")
        with threading.Lock():
            # print("\nPutting data\n", threadName, result[0])
            # print("Result Queue Put = "+ str(result_queue))
            result_queue.put((threadName, result))
            schema_queue.put(table_description)
            # print(result_queue.queue)
            # print('\nData Put\n')


    # Actual function starts here

    # Determine the numThreads
    numThreads = 5

    # Get the cursor
    cur = openconnection.cursor()

    # Divide the data in OutputTable into 5 parts. For each thread
    # Get the count. You are using the length so, adding 1 is not required while getting the slices
    numvalues = GetCount(cur, InputTable)

    # Divide the numvalues data rows equally among 5 Threads
    dataRowsPerThread = numvalues // numThreads

    # Get the data from the table
    # tabledata = GetData(cur, InputTable)
    # print(type(tabledata))
    # for d in tabledata:
    #     print(d)

    # print(len(tabledata))

    # Divide the data among Threads. Generate the splicing indexes
    dataspliceIndexes = GetSplicingIndex(numThreads, numvalues)

    # # Testing 
    # dataspliceIndexes = GetSplicingIndex(5, 10020)

    # Create Queue to store the data
    result_queue = Queue(maxsize=numThreads+2) # numThreads+1 for Threads and 1 for the None
    schema_queue = Queue(maxsize=numThreads+2)

    # Put the 'None' in the beginning
    # result_queue.put('None')

    # print("Result Queue Orig = "+ str(result_queue))

    # # Create Thread Pool and distribute the data to sort
    thread_pool = list(map(lambda interval: \
        threading.Thread(target=worker_sort, kwargs={'conn':openconnection,'tablename':InputTable, 'start_index':interval[0], 'end_index':interval[1], 'col':SortingColumnName}), \
        dataspliceIndexes))

    # Start the Thread and make daemon
    # map(lambda t: t.daemon=True, thread_pool)
    # map(lambda t: t.start(), thread_pool)


    for t in thread_pool:
        t.daemon = True
        t.start()
    
    for t in thread_pool:
        t.join()

    result_queue.put('None')
    

    # Get the partially sorted data
    partially_sorted_data_list = get_data_from_queue(result_queue, get_type='append')

    # Get the schema
    schema = schema_queue.get()


    # Perform merging on Sorted data
    merged_data = merge_data(numThreads, partially_sorted_data_list, schema, SortingColumnName)
    # print(merged_data)

    # Write to a file
    # with open('a.txt', 'w') as f:
    #     for m in merged_data:
    #         f.write(str(m))
    #         f.write('\n')


    # Write to the database
    write_to_database(openconnection, InputTable, OutputTable, schema, merged_data)

    pass #Remove this once you are done with implementation






def get_unique_values_table(openconnection, tablename, colname):
    q_select = """select {col} from {tablename}"""
    select_query = q_select.format(tablename=tablename, col=colname)

    cur = openconnection.cursor()

    cur.execute(select_query)

    values = cur.fetchall()

    values = list(map(lambda x: x[0], values))

    uniq_values = list(set(values))

    uniq_values.sort()

    # print(uniq_values)
    # sys.exit()

    return uniq_values




def get_join_values(table1_data=None, table2_data=None):

    from itertools import product
    return list(map(lambda x: x[0]+x[1], list(product(table1_data, table2_data))))


def get_table_description(table1=None, table1_desc=None, table2=None, table2_desc=None):

    return list(map(lambda x, t=table1: ".".join([t,x]), table1_desc)) + list(map(lambda x, t=table2: ".".join([t,x]), table2_desc))


def get_datatype(conn=None, table=None):

    cur = conn.cursor()
    q_type_select = """select column_name, data_type from information_schema.columns where table_name = '{table}';""".format(table=table)
    cur.execute(q_type_select)
    col_dtypes_tuple = cur.fetchall()
    return col_dtypes_tuple

def get_alias(dtype):

    dtype_dict = {
                    "bigint" : "int8",
                    "bigserial" : "serial8",
                    "boolean" : "bool",
                    "character" : "text",
                    "character varying" : "text",
                    "double precision" : "float8",
                    "integer" : "int",
                    "real" : "real",
                    "smallint" : "int2",
                    "smallserial" : "serial2",
                    "serial" : "serial4",
                    "timestamp" : "int",
                    "time" : "int",
                    "text" : "text",
    }

    return dtype_dict.get(dtype, "text")


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.

    # print("******************Starting Parallel Join***************")

    def worker_join(conn=None, table1=None, table2=None, col1=None, col2=None, start_index=None, end_index=None, uniq_values=None, thread_num=None):
        threadName = threading.current_thread().name

        # create the query
        q_select = """select * from {tablename} where {col} = {allcolvalues} ORDER BY {col};"""


        select_query_table1 = ''
        select_query_table2 = ''

        # Convert each values in uniq_value to string
        uniq_values_this_thread = uniq_values[start_index:end_index]
        uniq_values_this_thread = list(map(lambda x: str(x), uniq_values_this_thread))

        # with open('out/file_'+str(thread_num)+'.txt', 'w') as f:
        #     f.write('\n\n\nThread Name = {th}-{thn} \t Start Index = {st} \t End Index = {en}'.format(th=threadName, thn=thread_num, st=start_index, en=end_index) + "\n\n\n")
        #     # f.write(str(uniq_values[start_index]) + "\n")
        #     # f.write(str(uniq_values[end_index]) + "\n\n\n")
        #     # f.write(str(uniq_values[end_index]) + "\n\n\n\n")
        #     list(map(lambda x: f.write(str(x) + "\n"), uniq_values_this_thread))

        # sys.exit()

        # if thread_num == 1:

        #     # joining all values to format the query
        #     col_cond_values_tbl1 = " or {col} = ".join(uniq_values).format(col1)
        #     col_cond_values_tbl2 = " or {col} = ".join(uniq_values).format(col2)

        #     select_query_table1 = q_select.format(tablename=table1, col=col1, space_or_equal='=', start_val=start_index, end_val=end_index)
        #     select_query_table2 = q_select.format(tablename=table2, col=col2, space_or_equal='=', start_val=start_index, end_val=end_index)
        #     pass

        # else:

        #     # joining all values to format the query
        #     uniq_val_exclude_first = uniq_values[1:]
        #     col_cond_values_tbl1 = " or {col} = ".join(uniq_val_exclude_first).format(col1)
        #     col_cond_values_tbl2 = " or {col} = ".join(uniq_val_exclude_first).format(col2)

        #     select_query_table1 = q_select.format(tablename=table1, col=col1, space_or_equal=' ', start_val=start_index, end_val=end_index)
        #     select_query_table2 = q_select.format(tablename=table2, col=col2, space_or_equal=' ', start_val=start_index, end_val=end_index)
        #     pass


        # sys.exit()

        # Create the query
        col_cond_values_tbl1 = " or {col} = ".join(uniq_values_this_thread).format(col=col1)
        col_cond_values_tbl2 = " or {col} = ".join(uniq_values_this_thread).format(col=col2)

        select_query_table1 = q_select.format(tablename=table1, col=col1, allcolvalues=col_cond_values_tbl1)
        select_query_table2 = q_select.format(tablename=table2, col=col2, allcolvalues=col_cond_values_tbl2)
        pass


        

        cur = openconnection.cursor()

        with threading.Lock():
            cur.execute(select_query_table1)
        table1_desc = list(map(lambda x: x.name.lower(), cur.description))


        table1_values = cur.fetchall()

        with threading.Lock():
            cur.execute(select_query_table2)
        table2_desc = list(map(lambda x: x.name.lower(), cur.description))
        table2_values = cur.fetchall()

        # with threading.Lock():
        #     print('\n\n\nThread Name = {th}-{thn} \t Start Index = {st} \t End Index = {en}'.format(th=threadName, thn=thread_num, st=start_index, en=end_index))
        #     print("\n\n**********Table1************\n")
        #     print(table1_values)
        #     print("\n\n**********Table2************\n\n\n")
        #     print(table2_values)

        # with open('out/file_'+str(thread_num)+'.txt', 'w') as f:
        #     f.write('\n\n\nThread Name = {th}-{thn} \t Start Index = {st} \t End Index = {en}'.format(th=threadName, thn=thread_num, st=start_index, en=end_index))
        #     f.write("\n\n**********Table1************\n")
        #     list(map(lambda x: f.write(str(x) + "\n"), table1_values))
        #     f.write("\n\n**********Table2************\n\n\n")
        #     list(map(lambda x: f.write(str(x) + "\n"), table2_values))



        # Find the intersection / Join of two tables based on joining attribute
        # print(len(table1_values))
        # print(len(table2_values))

        # if length of table 1 or table 2 is 0. return empty list
        if len(table1_values) == 0 or len(table2_values) == 0:
            # put empty data to the queue
            return None

        # otherwise perform join of two tables
        
        # Get the join column index in both the tables
        # print("{desc} === {col}".format(desc=table1_desc, col=Table1JoinColumn))
        # print("{desc} === {col}".format(desc=table2_desc, col=Table2JoinColumn))
        tbl1_join_col_index = table1_desc.index(Table1JoinColumn.lower())
        tbl2_join_col_index = table2_desc.index(Table2JoinColumn.lower())

        # groupby both lists using the common column
        from itertools import groupby

        table1_grp = dict({key : list(group) for key, group in groupby(table1_values, key=lambda x:x[tbl1_join_col_index])})
        table2_grp = dict({key : list(group) for key, group in groupby(table2_values, key=lambda x:x[tbl2_join_col_index])})
        
        # print(table1_grp)

        # with open('out1/file_'+str(thread_num)+'.txt', 'w') as f:
        #     f.write('\n\n\nThread Name = {th}-{thn} \t Start Index = {st} \t End Index = {en}'.format(th=threadName, thn=thread_num, st=start_index, en=end_index))
        #     f.write("\n\n**********Table1************\n")
        #     list(map(lambda x: f.write("{k}\t{v}\n".format(k=x, v=table1_grp[x])), table1_grp))
        #     f.write("\n\n**********Table2************\n\n\n")
        #     list(map(lambda x: f.write("{k}\t{v}\n".format(k=x, v=table2_grp[x])), table2_grp))


        # Get the join data
        joined_values_list = []
        for join_col_val, group_vals in table1_grp.iteritems():

            if join_col_val in table2_grp.keys():

                joined_values = get_join_values(table1_data=table1_grp[join_col_val] , table2_data=table2_grp[join_col_val])
                joined_values_list.extend(joined_values)



        # Get the table description
        table_description = get_table_description(table1=table1, table1_desc=table1_desc, table2=table2, table2_desc=table2_desc)


        # # Save data to the flie
        # with open('out2/file_'+str(thread_num)+'.txt', 'w') as f:
        #     f.write('\n\n\nThread Name = {th}-{thn} \t Start Index = {st} \t End Index = {en}'.format(th=threadName, thn=thread_num, st=start_index, en=end_index))
        #     f.write("\n\n**********Table_Join************\n")
        #     f.write('\t'.join(table_description) + "\n\n")
        #     list(map(lambda x: f.write(str(x) + "\n\n\n"), joined_values_list))


        # Acquire the lock before putting the data to the Queue
        # print("Acquiring Lock")
        with threading.Lock():
            join_result_queue.put((threadName, joined_values_list))
            join_schema_queue.put(table_description)
            



    # Define numThreads
    numThreads = 5

    # Create Queue to store the data
    join_result_queue = Queue(maxsize=numThreads+2) # numThreads+1 for Threads and 1 for the None
    join_schema_queue = Queue(maxsize=numThreads+2)

    # Let's start with Table1JoinColumn. Get the unique values from the column
    table1_uniq_values = get_unique_values_table(openconnection, InputTable1, Table1JoinColumn)
    num_uniq_values = len(table1_uniq_values)

    # Divide the unique values in numThreads equal partitions to assign to each thread
    splices_in_col_values = GetSplicingIndex(numThreads, num_uniq_values)


    # # Create Thread Pool and distribute the data to sort
    thread_pool = list(map(lambda index_interval: \
        threading.Thread(target=worker_join, kwargs={'conn':openconnection,'table1':InputTable1, 'table2':InputTable2, 'col1': Table1JoinColumn, 'col2': Table2JoinColumn, 'start_index':index_interval[1][0], 'end_index':index_interval[1][1], 'uniq_values':table1_uniq_values, 'thread_num':index_interval[0] + 1}), \
        enumerate(splices_in_col_values) ))

    for t in thread_pool:
        t.start()

    for t in thread_pool:
        t.join()


    join_result_queue.put('None')

    # Get the data from the queue
    joined_data = get_data_from_queue(join_result_queue, get_type='extend')
    # with open('out3/file_myf'+'.txt', 'w') as f:
    #     f.write("\n\n**********Table_Join************\n")
    #     # f.write('\t'.join(table_description) + "\n\n")
    #     list(map(lambda x: f.write(str(x) + "\n\n\n\n\n\n"), joined_data))

    
    # Get the schema
    schema_data = join_schema_queue.get()

    # Get the data type of the schemas in the two table
    tbl1_datatype = get_datatype(conn=openconnection, table=InputTable1)
    tbl2_datatype = get_datatype(conn=openconnection, table=InputTable2)

    # print(tbl1_datatype) # [('userid', 'integer'), ('movieid', 'integer'), ('rating', 'real')]

    # print(tbl2_datatype) # [('movieid1', 'integer'), ('title', 'character varying'), ('genre', 'character varying')]

    # Combine both
    tbl_dtype_dict = {col: get_alias(dtype) for col, dtype in tbl1_datatype+tbl2_datatype}


    # Insert the data into the table
    write_to_database_join(openconnection=openconnection, output_table_name=OutputTable, schema=schema_data, dtype_dict=tbl_dtype_dict, joined_data=joined_data)


    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
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
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
