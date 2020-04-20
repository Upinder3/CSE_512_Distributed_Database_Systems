
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Assumption: Partition table names have prefix  like 'rangeratingspart%' or 'roundrobinratingspart%'
    try:
        cur = openconnection.cursor()
        
        #Only selecting the range tables which have data of range ratingMaxValue to ratingMaxValue.
        cur.execute("""SELECT * FROM RangeRatingsMetadata where minrating <= {1} and maxrating >= {0}""".format(ratingMinValue, ratingMaxValue))
        
        partitionnum = cur.fetchall()
        partition_tables = []
        
        #Storing the rangeparition tables in partition_tables list
        for p in partitionnum:
            partition_tables.append('rangeratingspart' + str(p[0]))
    
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and lower(table_name) like 'roundrobinratingspart%'")
        round_tables = cur.fetchall()
        
        #Storing the round robin parition tables in partition_tables list
        for r in round_tables:
            partition_tables.append(r[0])
        
        with open(outputPath, 'w') as of:
            for table in partition_tables:
                cur.execute("SELECT * FROM {0} WHERE rating >= {1} and rating <= {2}".format(table, ratingMinValue, ratingMaxValue))
                rows = cur.fetchall()
                for row in rows:
                    of.write("{0},{1},{2},{3}\n".format(table, row[0], row[1], row[2]))
    
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cur:
            cur.close()
        
def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    #Assumption: Partition table names have prefix  like 'rangeratingspart%' or 'roundrobinratingspart%'
    try:
        cur = openconnection.cursor()
        
        #Only selecting the range tables in which the ratingValue lies.
        cur.execute("""SELECT * FROM RangeRatingsMetadata where minrating <= {0} and maxrating >= {0}""".format(ratingValue))
        partitionnum = cur.fetchall()
        
        partition_tables = []
        #Storing the rangeparition tables in partition_tables list
        for p in partitionnum:
            partition_tables.append('rangeratingspart' + str(p[0]))
    
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and lower(table_name) like 'roundrobinratingspart%'")
        round_tables = cur.fetchall()
        
        #Storing the round robin parition tables in partition_tables list
        for r in round_tables:
            partition_tables.append(r[0])
        
        with open(outputPath, 'w') as of:
            for table in partition_tables:
                cur.execute("SELECT * FROM {0} WHERE rating = {1}".format(table, ratingValue))
                rows = cur.fetchall()
                for row in rows:
                    of.write("{0},{1},{2},{3}\n".format(table, row[0], row[1], row[2]))

    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cur:
            cur.close()