#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
        create_command =  "create table {} (userid integer not null , movieid integer not null, rating float not null check(rating>=0 and rating<=5), constraint R_PK primary key (userid, movieid))".format(ratingstablename)

        cur.execute(create_command)
        file = open(ratingsfilepath, "r")
        for line in file:
            line = line.split("::")
            userid = int(line[0])
            movieid = int(line[1])
            rating = float(line[2])
            insert_command = "insert into " + ratingstablename + " (userid,movieid,rating) values (%s,%s,%s);"
            cur.execute(insert_command,(userid, movieid, rating,))

        file.close()
        openconnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print ("Error while creating PostgreSQL table", error)
    finally:
        if (cur):
            cur.close()

def get_ranges(list, numberofpartitions, partition_range):
    for i in range(numberofpartitions):
        list.append(tuple([i, (i * partition_range), ((i + 1) * partition_range)]))


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cur = openconnection.cursor()
        partition_range = 5.0/numberofpartitions

        list = []
        get_ranges(list, numberofpartitions, partition_range)
        for i in range(numberofpartitions):
            new_table_name = "{}{}".format(RANGE_TABLE_PREFIX, str(i))
            create_command = "create table if not exists {} (userid integer not null , movieid integer not null, rating float not null check(rating>=0 and rating<=5), primary key (userid, movieid))".format(new_table_name)
            cur.execute(create_command)

        select_command = "select * from {}".format(ratingstablename)
        cur.execute(select_command)
        res_table = cur.fetchall()

        for each_command in res_table:
            for idx,start,end in list:
                if idx == 0:
                    if (float(start) <= float(each_command[2]) <= float(end)):
                        new_table_name = "{}{}".format(RANGE_TABLE_PREFIX, str(idx))
                        insert_command = "insert into " + new_table_name + " (userid,movieid,rating) values (%s,%s,%s);"
                        cur.execute(insert_command, (each_command[0], each_command[1], each_command[2],))
                        break
                else:
                    if (float(start) < float(each_command[2]) <= float(end)):
                        new_table_name = "{}{}".format(RANGE_TABLE_PREFIX, str(idx))
                        insert_command = "insert into " + new_table_name + " (userid,movieid,rating) values (%s,%s,%s);"
                        cur.execute(insert_command, (each_command[0], each_command[1], each_command[2],))
                        break

        openconnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print ("Error while creating range partition table", error)
    finally:
        if (cur):
            cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cur = openconnection.cursor()

        for i in range(numberofpartitions):
            new_table_name = "{}{}".format(RROBIN_TABLE_PREFIX, str(i))
            create_command = "create table if not exists {} (userid integer not null , movieid integer not null, rating float not null check(rating>=0 and rating<=5), primary key (userid, movieid))".format(new_table_name)
            cur.execute(create_command)

        select_command = "select * from {}".format(ratingstablename)
        cur.execute(select_command)
        res_table = cur.fetchall()

        for record_num, each_command in enumerate(res_table):
            idx = record_num%numberofpartitions
            new_table_name = "{}{}".format(RROBIN_TABLE_PREFIX, str(idx))
            insert_command = "insert into " + new_table_name + " (userid,movieid,rating) values (%s,%s,%s);"
            cur.execute(insert_command, (each_command[0], each_command[1], each_command[2],))

        openconnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print ("Error while creating roundrobin partition table", error)
    finally:
        if (cur):
            cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        insert_command = "insert into " + ratingstablename + " (userid,movieid,rating) values (%s,%s,%s);"
        cur.execute(insert_command, (int(userid), int(itemid), float(rating),))

        select_command = "select count(*) from {}".format(ratingstablename)
        cur.execute(select_command)
        new_count = cur.fetchone()

        count_command = """SELECT count(*) FROM information_schema.tables WHERE table_name like \'""" + RROBIN_TABLE_PREFIX+'%'+"""\' """
        cur.execute(count_command)
        num_partitions = cur.fetchone()

        idx = (new_count[0] % num_partitions[0])-1

        new_table_name = "{}{}".format(RROBIN_TABLE_PREFIX, str(idx))
        insert_command = "insert into " + new_table_name + " (userid,movieid,rating) values (%s,%s,%s);"
        cur.execute(insert_command, (int(userid), int(itemid), float(rating),))

        openconnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print ("Error while roundrobin insert into a table", error)
    finally:
        if (cur):
            cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        insert_command = "insert into " + ratingstablename + " (userid,movieid,rating) values (%s,%s,%s);"
        cur.execute(insert_command, (int(userid), int(itemid), float(rating),))
        count_command = """SELECT count(*) FROM information_schema.tables WHERE table_name like \'""" + RANGE_TABLE_PREFIX+'%'+"""\' """
        cur.execute(count_command)
        num_partitions = cur.fetchone()

        partition_range = 5.0 / num_partitions[0]
        list = []
        get_ranges(list, num_partitions[0], partition_range)

        for idx,start,end in list:
            if idx == 0:
                if (float(start) <= float(rating) <= float(end)):
                    table_name = "{}{}".format(RANGE_TABLE_PREFIX, str(idx))
                    insert_command = "insert into " + table_name + " (userid,movieid,rating) values (%s,%s,%s);"
                    cur.execute(insert_command, (int(userid), int(itemid), float(rating),))
                    break
            else:
                if (float(start) < float(rating) <= float(end)):
                    table_name = "{}{}".format(RANGE_TABLE_PREFIX, str(idx))
                    insert_command = "insert into " + table_name + " (userid,movieid,rating) values (%s,%s,%s);"
                    cur.execute(insert_command, (int(userid), int(itemid), float(rating),))
                    break

        openconnection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print ("Error while range insert into a table", error)
    finally:
        if (cur):
            cur.close()

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