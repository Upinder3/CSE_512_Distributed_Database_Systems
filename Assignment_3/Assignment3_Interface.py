#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation
    try:
        cur = openconnection.cursor()
        cur.execute("SELECT max({0}) FROM {1}".format(str(SortingColumnName), str(InputTable)))
        max_value = cur.fetchone()[0]
        
        cur.execute("SELECT min({0}) FROM {1}".format(str(SortingColumnName), str(InputTable)))
        min_value = cur.fetchone()[0]
        
        diff = max_value - min_value
        diff = diff / 5.0
        thread_list = []
        
        temp_table_prefix = "table_thread_"
        drop_temp_tables(openconnection, temp_table_prefix)
        
        for i in range(5):
            thread_list.append(threading.Thread(target=parallel_sort_helper(temp_table_prefix + str(i), InputTable, \
                                                                min_value, min_value + diff, SortingColumnName, max_value, openconnection)))
            thread_list[i].start()
            min_value = min_value + diff
        
        for t in thread_list:
            t.join()
        
        cur.execute("DROP TABLE IF EXISTS " + OutputTable)
        cur.execute("""CREATE TABLE {0} AS (SELECT * FROM {1}0  union all  
                                            SELECT * FROM {1}1 union all 
                                            SELECT * FROM {1}2 union all 
                                            SELECT * FROM {1}3 union all 
                                            SELECT * FROM {1}4)""".format(OutputTable,temp_table_prefix))
        
        drop_temp_tables(openconnection, temp_table_prefix)
        openconnection.commit()

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
            
def drop_temp_tables(connection, tbl):
    cur = connection.cursor()
    for i in range(5):
        cur.execute("DROP TABLE IF EXISTS "+ tbl + str(i))


def parallel_sort_helper(temp_tbl, inputTable, lower, upper, column, max_value, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ str(temp_tbl))
    
    if (upper == max_value):
        cur.execute("""CREATE TABLE {0} AS SELECT * FROM {1} 
                        WHERE {2} >= {3} AND {2} <= {4} ORDER BY {2}""".format(str(temp_tbl), str(inputTable), \
                        str(column), str(lower), str(upper)))
    else:
        cur.execute("""CREATE TABLE {0} AS SELECT * FROM {1} 
                        WHERE {2} >= {3} AND {2} < {4} ORDER BY {2}""".format(str(temp_tbl), str(inputTable), \
                        str(column), str(lower), str(upper)))


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation
    try:
        cur= openconnection.cursor()
    
        cur.execute("SELECT max({0}) FROM {1}".format(str(Table1JoinColumn), str(InputTable1)))
        max_value = cur.fetchone()[0]
    
        cur.execute("SELECT min({0}) FROM {1}".format(str(Table1JoinColumn), str(InputTable1)))
        min_value = cur.fetchone()[0]
    
        cur.execute("SELECT max({0}) FROM {1}".format(str(Table2JoinColumn), str(InputTable2)))
        max_value = max(max_value, cur.fetchone()[0])
    
        cur.execute("SELECT min({0}) FROM {1}".format(str(Table2JoinColumn), str(InputTable2)))
        min_value = min(min_value, cur.fetchone()[0])
    
        diff = max_value - min_value
        diff = diff / 5.0
        thread_list = []
    
        temp_table_prefix = "table_thread_"
        drop_temp_tables(openconnection, temp_table_prefix)
        
        for i in range(5):
            thread_list.append(threading.Thread(target=parallel_join_helper, args=(
                temp_table_prefix + str(i), InputTable1,InputTable2, min_value, min_value + diff, Table1JoinColumn, Table2JoinColumn, max_value , openconnection)))
            thread_list[i].start()
            min_value = min_value + diff
            
        for t in thread_list:
            t.join()
            
        cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    
        cur.execute("""CREATE TABLE {0} AS (SELECT * FROM {1}0  union all  
                                            SELECT * FROM {1}1 union all 
                                            SELECT * FROM {1}2 union all 
                                            SELECT * FROM {1}3 union all 
                                            SELECT * FROM {1}4)""".format(OutputTable,temp_table_prefix))
        
        drop_temp_tables(openconnection, temp_table_prefix)
        openconnection.commit()
        
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
            
            
def parallel_join_helper(temp_tbl, InputTable1, InputTable2, lower, upper, Table1JoinColumn, Table2JoinColumn, max_value, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+str(temp_tbl))
    
    if (upper == max_value):
        cur.execute("""CREATE TABLE {0} AS SELECT * FROM {1} t1 INNER JOIN {2} t2 
                    ON t1.{3} = t2.{4} WHERE {3} >= {5} AND {3} <= {6} 
                    AND {4} >= {5} AND {4} <= {6}""".format(temp_tbl, InputTable1, InputTable2, Table1JoinColumn, \
                    Table2JoinColumn, lower, upper))
    else:        
        cur.execute("""CREATE TABLE {0} AS SELECT * FROM {1} t1 INNER JOIN {2} t2 
                    ON t1.{3} = t2.{4} WHERE {3} >= {5} AND {3} < {6} 
                    AND {4} >= {5} AND {4} < {6}""".format(temp_tbl, InputTable1, InputTable2, Table1JoinColumn, \
                    Table2JoinColumn, lower, upper))

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
        print('A database named {0} already exists'.format(dbname))

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
        if cursor:
            cursor.close()


