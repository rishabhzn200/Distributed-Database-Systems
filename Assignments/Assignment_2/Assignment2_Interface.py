#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()



def liesbetween(val, rmin, rmax):
	return (val >= rmin) and (val <= rmax)

def inrange(qmin=None, qmax=None, rmin=None, rmax=None):
	# check if rmin and rmax are inside range qmin and qmax
	return (qmin <= rmin) and (qmax >= rmax)

def checkPartition(qmin=None, qmax=None, rmin=None, rmax=None):
	return liesbetween(qmin, rmin, rmax) or liesbetween(qmax, rmin, rmax) or inrange(qmin, qmax, rmin, rmax)


def getdata(cur, ratingMinValue, ratingMaxValue, partitionstables, mode='range'):

	qselect = ""

	if mode == 'range':
		qselect = """select * from {tablename} where rating >= {rmin} and rating <= {rmax};"""

	else:
		qselect = """select * from {tablename} where rating = {pointval};"""

	# print "Partitions table are: "
	# print partitionstables

	for partitiontable in partitionstables:

		if mode == 'range':
			qselectformat = qselect.format(tablename=partitiontable, rmin=ratingMinValue, rmax=ratingMaxValue)
		else:
			qselectformat = qselect.format(tablename=partitiontable, pointval=ratingMinValue)


		# print 'Executing query: {q}'.format(q=qselectformat)
		cur.execute(qselectformat)
		values = cur.fetchall()
		fetcheddata = map(lambda y: partitiontable + ',' + ','.join(map(lambda x: str(x),y)) + '\n',values)
		# print fetcheddata
		# print "Yielding Data from partition: "
		# print fetcheddata
		yield fetcheddata


def GetRoundRobinPartitions(cur, metadata=None, prefix=None):
	# print metadata
	numpartition, nextpartition = metadata[0]
	partitionsnames = list(map(lambda x: prefix + str(x), range(numpartition)))

	return partitionsnames



def GetMetadata(cur, tablename=''):
	q_select = """select * from {tablename}""".format(tablename=tablename)
	cur.execute(q_select)
	return cur.fetchall()

def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.


    # print "**********Starting Range Query*********************"
    #import collections.namedtuple as nt

    range_part_name_prefix = 'rangeratingspart'
    rrobin_part_name_prefix = 'roundrobinratingspart'

    # Get the cursor
    cur = openconnection.cursor()

    # Get the data from Metadata
    metadataTables = ['roundrobinratingsmetadata', 'rangeratingsmetadata']

    # Get the metadata table values
    # try:
    metadata = list(map(lambda x: GetMetadata(cur, tablename=x), metadataTables))
    # print "{metadata}".format(metadata=metadata)

    # except:
    # 	print("Error Occured!!")
    # 	import sys
    # 	sys.exit()

    # metadata is [[(5, 0)], [(0, 0.0, 1.0), (1, 1.0, 2.0), (2, 2.0, 3.0), (3, 3.0, 4.0), (4, 4.0, 5.0)]]

    roundrobinmetadata, rangemetadata = metadata

    # RoundRobin
    # get the roundrobin partitions
    partitionstables_rrobin = GetRoundRobinPartitions(cur, metadata=roundrobinmetadata, prefix=rrobin_part_name_prefix)
    # print partitionstables_rrobin

    # Range
    # filter partitions
    #rangepartitionmetadata = nt(partitionnum, minrating, maxrating)

    # Get the partitions with ratings in between min and max rating. row = partitionnum, minrating, maxrating
    # searchpartitionsindex = list(filter(lambda row: liesbetween(val=ratingMinValue, rmin=row[1], rmax=row[2]) or liesbetween(val=ratingMaxValue, rmin=row[1], rmax=row[2]), rangemetadata))
    searchpartitionsindexrows = list(filter(lambda row: checkPartition(qmin=ratingMinValue, qmax=ratingMaxValue, rmin=row[1], rmax=row[2]), rangemetadata))
    # print "{sp}".format(sp=searchpartitionsindexrows)


    # Get the queries
    
    partitionstables_range = list(map(lambda x: range_part_name_prefix + str(x[0]), searchpartitionsindexrows))

    # print partitionstables_

    # Fetch data from the table
    datageneratorrrobin = getdata(cur, ratingMinValue, ratingMaxValue, partitionstables_rrobin, mode='range')
    datageneratorrange = getdata(cur, ratingMinValue, ratingMaxValue, partitionstables_range, mode='range')

    with open(outputPath, 'w') as f:
    	for data in datageneratorrrobin:
    		for line in data:
    			f.write(line)

    	for data in datageneratorrange:
    		for line in data:
    			f.write(line)





    pass #Remove this once you are done with implementation

def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    # print "**********Starting Point Query*********************"
    #import collections.namedtuple as nt

    range_part_name_prefix = 'rangeratingspart'
    rrobin_part_name_prefix = 'roundrobinratingspart'

    # Get the cursor
    cur = openconnection.cursor()

    # Get the data from Metadata
    metadataTables = ['roundrobinratingsmetadata', 'rangeratingsmetadata']

    # Get the metadata table values
    # try:
    metadata = list(map(lambda x: GetMetadata(cur, tablename=x), metadataTables))

    roundrobinmetadata, rangemetadata = metadata


    # get the roundrobin partitions
    partitionstables_rrobin = GetRoundRobinPartitions(cur, metadata=roundrobinmetadata, prefix=rrobin_part_name_prefix)
    # print partitionstables_rrobin


    searchpartitionsindexrows = list(filter(lambda row: liesbetween(val=ratingValue, rmin=row[1], rmax=row[2]), rangemetadata))
    # print "{sp}".format(sp=searchpartitionsindexrows)


    # Get the queries
    
    partitionstables_range = list(map(lambda x: range_part_name_prefix + str(x[0]), searchpartitionsindexrows))

    # print partitionstables_

    # Fetch data from the table
    datageneratorrrobin = getdata(cur, ratingValue, None, partitionstables_rrobin, mode='point')
    datageneratorrange = getdata(cur, ratingValue, None, partitionstables_range, mode='point')

    with open(outputPath, 'w') as f:
    	for data in datageneratorrrobin:
    		for line in data:
    			f.write(line)

    	for data in datageneratorrange:
    		for line in data:
    			f.write(line)

    pass # Remove this once you are done with implementation

