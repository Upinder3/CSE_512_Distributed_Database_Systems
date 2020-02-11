import psycopg2

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


#Generic method to create a ratings table.
def createPartitionTable(tableName, cursor):
    cursor.execute("DROP TABLE IF EXISTS {0}".format(tableName))
    tbl_cmd = "CREATE TABLE {0} (userid int, movieid int,rating float)".format(tableName)
    cursor.execute(tbl_cmd)
    
def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    
    cur = openconnection.cursor()
    rf_handle = open(ratingsfilepath, 'r')
    
    #Instead of altering the file or creating an additional table. Creating temporary columns for the additional data in the file.
    cur.execute("CREATE TABLE {0} (userid int, temp1 varchar, movieid int, temp2 varchar, rating float, temp3 varchar, temp4 varchar)".format(ratingstablename))
    
    #Handling empty values by replacing it with null. Using null='' option. This option replaces empty strings with NULL.
    cur.copy_from(rf_handle,ratingstablename,sep=":", null='')
    
    #Droping temporary columns.
    drop_column = 'ALTER TABLE {} DROP temp1, DROP temp2, DROP temp3, DROP temp4'.format(ratingstablename)
    cur.execute(drop_column)
    
    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()
    cur.close()
    

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions<=1:
        return

    start = 0.0
    interval = 5.0/numberofpartitions

    
    cur = openconnection.cursor()
    createPartitionTable(RANGE_TABLE_PREFIX + '0', cur)
    #Need to handle rating == 0.0 explicitly because it also includes the lower limit.
    cur.execute('INSERT INTO {0} SELECT * from {1} WHERE rating >= {2} AND rating <= {3}'.format(RANGE_TABLE_PREFIX + '0', ratingstablename, start, start+interval))

    start += interval
    #for partitions from 1 to (number of partitions - 1)
    for p in range(1, numberofpartitions):
        createPartitionTable(RANGE_TABLE_PREFIX + str(p), cur)
        cur.execute('INSERT INTO {0} SELECT * from {1} WHERE rating > {2} AND rating <= {3}'.format(RANGE_TABLE_PREFIX + str(p), ratingstablename, start, start+interval))
        start += interval
    
    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()
    cur.close()
    

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for p in range(0, numberofpartitions):
        createPartitionTable(RROBIN_TABLE_PREFIX + str(p), cur)

    #Could have taken all the data from ratings taken in the momery and then start inserting.
    #But instead I choose to select multiple times from the ratings table.
    for p in range(0, numberofpartitions):
        insert_cmd = """INSERT INTO {0} SELECT userid, movieid, rating FROM 
        (SELECT *, ROW_NUMBER() OVER (ORDER BY userid, movieid, rating) AS id 
        FROM {1}) AS tmp_tbl WHERE (id) % {2} = {3}""".format(RROBIN_TABLE_PREFIX+str(p),ratingstablename, str(numberofpartitions), str(p))

        cur.execute(insert_cmd)
        
    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()
    cur.close()
    

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format(RROBIN_TABLE_PREFIX))
    #Number of paritions is equal to the number of round robin tables.
    partition = cur.fetchone()[0]

    cur.execute("select count(*) from {}".format(ratingstablename))
    #Count of rows in ratings table
    ratings_cnt = cur.fetchone()[0]
    
    #partition number is the table in which the incoming row should be added.
    #Say there are 500,000 rows which are already partitioned then 500,001 row should be inserted in first partition.
    #(current row count + additional row) % number of partitions.
    partition_number = (ratings_cnt)%partition

    #Inserting in ratingstable and round robin partition.
    cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format(ratingstablename, str(userid), str(itemid), str(rating)))
    cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format(RROBIN_TABLE_PREFIX + str(partition_number), str(userid), str(itemid), str(rating)))
    
    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()
    cur.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("select count(*) from (SELECT * FROM information_schema.tables WHERE table_schema = 'public') as temp where table_name like '{}%'".format(RANGE_TABLE_PREFIX))
    #Number of paritions is equal to the number of range partition tables.
    partition = cur.fetchone()[0]
    
    start = 0.0
    interval = 5.0/partition
    
    #Inserting in ratings table.
    cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format( ratingstablename, str(userid), str(itemid), str(rating)))

    #Need to handle rating == 0.0 explicitly because it also includes the lower limit.
    if (rating==0.0):
        cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format( RANGE_TABLE_PREFIX+'0', str(userid), str(itemid), str(rating)))
    else:
        for p in range(partition):
            if(rating > start and rating <= start + interval):
                cur.execute('INSERT INTO {0} VALUES ({1}, {2}, {3})'.format( RANGE_TABLE_PREFIX+str(p), str(userid), str(itemid), str(rating)))
                #Break from the loop because we already found the desired range.
                break
            start += interval

    #Committing the changes just in case if the autocommit is not on in the calling script.
    openconnection.commit()       
    cur.close()
    

def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
