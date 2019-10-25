#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

def get_range(InputTable, SortingColumnName, numthreads, cursor):
    cursor.execute("SELECT MIN(" + SortingColumnName + ") , MAX( " + SortingColumnName + ") FROM "+InputTable)
    minval, maxval = cursor.fetchone()
    interval = float(maxval - minval)/numthreads
    return interval, minval, maxval

def sort_tables(InputTable, SortingColumnName, index, minval, maxval, cursor):
    table_name = "tablepart_{}".format(index)
    if index == 0:
        query = "INSERT INTO "+ table_name + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(minval) + " AND " + SortingColumnName + " <= "+ str(maxval) + " ORDER BY "+ SortingColumnName + " ASC "
    else:
        query = "INSERT INTO "+ table_name + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(minval) + " AND " + SortingColumnName + " <= " + str(maxval) + " ORDER BY " + SortingColumnName + " ASC "

    cursor.execute(query)

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    try:
        cursor = openconnection.cursor()
        numthreads = 5
        for i in range(numthreads):
            table_name = "tablepart_{}".format(i)
            cursor.execute("DROP TABLE IF EXISTS "+table_name)
            cursor.execute("CREATE TABLE " + table_name + " ( LIKE " + InputTable + " INCLUDING ALL )")

        interval, minval, maxval = get_range(InputTable, SortingColumnName, numthreads, cursor)
        #print(interval, minval, maxval)

        threads = [0] * numthreads
        for i in range(len(threads)):
            if i == 0:
                lower_bound = minval
                upper_bound = minval+interval
            else:
                lower_bound = upper_bound
                upper_bound = lower_bound+interval
            threads[i] = threading.Thread(target = sort_tables, args=(InputTable, SortingColumnName, i, lower_bound, upper_bound, cursor))
            threads[i].start()

        for i in range(len(threads)):
            threads[i].join()

        cursor.execute("DROP TABLE IF EXISTS "+ OutputTable)
        cursor.execute("CREATE TABLE " + OutputTable + " ( LIKE " + InputTable + " INCLUDING ALL )")

        for i in range(numthreads):
            table_name = "tablepart_{}".format(i)
            cursor.execute("INSERT INTO "+OutputTable+" SELECT * FROM "+table_name)

        for i in range(numthreads):
            table_name = "tablepart_{}".format(i)
            cursor.execute("DROP TABLE IF EXISTS "+table_name)

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

def minmax(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, numthreads, cursor):
    cursor.execute("SELECT MIN(" + Table1JoinColumn + ") , MAX( " + Table1JoinColumn + ") FROM "+InputTable1)
    minval1, maxval1 = cursor.fetchone()

    cursor.execute("SELECT MIN(" + Table2JoinColumn + ") , MAX( " + Table2JoinColumn + ") FROM "+InputTable2)
    minval2, maxval2 = cursor.fetchone()

    minval = min(minval1, minval2)
    maxval = max(maxval1, maxval2)

    interval = float(maxval - minval)/numthreads

    return interval, minval, maxval


def join_tables(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, index, minval, maxval, cursor):
    table_name1 = "tablepart1_{}".format(index)
    table_name2 = "tablepart2_{}".format(index)
    temp_OutTable = "temp_OutputTable_{}".format(index)
    if index == 0:
        query1 = "INSERT INTO " + table_name1 + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(
            minval) + " AND " + Table1JoinColumn + " <= " + str(maxval)
        query2 = "INSERT INTO " + table_name2 + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(
            minval) + " AND " + Table2JoinColumn + " <= " + str(maxval)
    else:
        query1 = "INSERT INTO " + table_name1 + " SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(
            minval) + " AND " + Table1JoinColumn + " <= " + str(maxval)
        query2 = "INSERT INTO " + table_name2 + " SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(
            minval) + " AND " + Table2JoinColumn + " <= " + str(maxval)
    cursor.execute(query1)
    cursor.execute(query2)
    query = "INSERT INTO " + temp_OutTable + " SELECT * FROM " + table_name1 + " INNER JOIN " + table_name2 + " ON " + table_name1 + "." + Table1JoinColumn + " = " + table_name2 + "." + Table2JoinColumn +";"
    cursor.execute(query)
    #print(query)


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    try:
        cursor = openconnection.cursor()

        cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'"+ InputTable1 +"\'")
        table1_schema = cursor.fetchall()

        cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'" + InputTable2 + "\'")
        table2_schema = cursor.fetchall()

        numthreads = 5
        for i in range(numthreads):
            table_name1 = "tablepart1_{}".format(i)
            cursor.execute("DROP TABLE IF EXISTS "+table_name1)
            cursor.execute("CREATE TABLE " + table_name1 + " ( LIKE " + InputTable1 + " INCLUDING ALL )")
            table_name2 = "tablepart2_{}".format(i)
            cursor.execute("DROP TABLE IF EXISTS " + table_name2)
            cursor.execute("CREATE TABLE " + table_name2 + " ( LIKE " + InputTable2 + " INCLUDING ALL )")
            temp_OutTable = "temp_OutputTable_{}".format(i)
            cursor.execute("DROP TABLE IF EXISTS " + temp_OutTable)
            cursor.execute("CREATE TABLE " + temp_OutTable + " ( " + table1_schema[0][0] + " " + table1_schema[0][1] + " )")
            for j in range(1, len(table1_schema)):
                cursor.execute("ALTER TABLE " + temp_OutTable + " ADD COLUMN " + table1_schema[j][0] + " " +
                               table1_schema[j][1])
            for j in range(0, len(table2_schema)):
                cursor.execute("ALTER TABLE " + temp_OutTable + " ADD COLUMN " + table2_schema[j][0] + " " +
                               table2_schema[j][1])

        interval, minval, maxval = minmax(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, numthreads, cursor)
        #print(interval, minval, maxval)

        threads = [0] * numthreads
        for i in range(len(threads)):
            if i == 0:
                lower_bound = minval
                upper_bound = minval+interval
            else:
                lower_bound = upper_bound
                upper_bound = lower_bound+interval
            threads[i] = threading.Thread(target = join_tables, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, i, lower_bound, upper_bound, cursor))
            threads[i].start()

        for i in range(len(threads)):
                threads[i].join()

        cursor.execute("DROP TABLE IF EXISTS " + OutputTable)
        cursor.execute("CREATE TABLE " + OutputTable + " ( " + table1_schema[0][0] + " " + table1_schema[0][1] + " )")
        for i in range(1, len(table1_schema)):
            cursor.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + table1_schema[i][0] + " " + table1_schema[i][1])
        for i in range(0, len(table2_schema)):
            cursor.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + table2_schema[i][0] + " " + table2_schema[i][1])

        for i in range(numthreads):
            temp_OutTable = "temp_OutputTable_{}".format(i)
            cursor.execute("INSERT INTO "+OutputTable+" SELECT * FROM "+temp_OutTable)

        # file = open("outputfile","w+")
        # cursor.execute("SELECT * FROM "+OutputTable+";")
        # table = cursor.fetchall()
        # for each_command in table:
        #     file.write(str(each_command)+'\n')
        #
        # file2 = open("expected","w+")
        # cursor.execute("select * from "+ InputTable1 +"," +InputTable2 + " where " + InputTable1 +"."+Table1JoinColumn+" = "+InputTable2 +"."+Table2JoinColumn)
        # table2 = cursor.fetchall()
        # for each_command in table2:
        #     file2.write(str(each_command) + '\n')
        #
        # file.close()
        # file2.close()

        for i in range(numthreads):
            table_name1 = "tablepart1_{}".format(i)
            table_name2 = "tablepart2_{}".format(i)
            temp_OutTable = "temp_OutputTable_{}".format(i)
            cursor.execute("DROP TABLE IF EXISTS " + table_name1)
            cursor.execute("DROP TABLE IF EXISTS " + table_name2)
            cursor.execute("DROP TABLE IF EXISTS "+ temp_OutTable)

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
