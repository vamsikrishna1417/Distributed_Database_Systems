#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'rangeratingspart'
RROBIN_TABLE_PREFIX = 'roundrobinratingspart'
# Donot close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery_Write_Function(file, num_partitions, table_prefix, minval, maxval, cursor):
    for idx in range(num_partitions):
        table_name = "{}{}".format(table_prefix, idx)
        cursor.execute("SELECT * from {}".format(table_name))
        res_table = cursor.fetchall()
        for each_command in res_table:
            if (minval <= each_command[2] <= maxval):
                file.write(table_name + ',' + str(int(each_command[0])) + ',' + str(int(each_command[1])) + ',' + str(
                    float(each_command[2])) + '\n')

def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    try:
        cursor = openconnection.cursor()
        file = open(outputPath, "w+")

        cursor.execute(" select * from RoundRobinRatingsMetadata")
        rrobin_partitions = cursor.fetchone()
        RangeQuery_Write_Function(file, rrobin_partitions[0], RROBIN_TABLE_PREFIX, ratingMinValue, ratingMaxValue, cursor)

        cursor.execute("select * from  RangeRatingsMetadata")
        metadata = cursor.fetchall()
        for command in metadata:
            minrating = command[1]
            maxrating = command[2]
            if(ratingMinValue > maxrating):
                continue
            if(ratingMaxValue < minrating):
                break
            else:
                table_name = "{}{}".format(RANGE_TABLE_PREFIX, command[0])
                cursor.execute("SELECT * from {}".format(table_name))
                res_table = cursor.fetchall()
                for each_command in res_table:
                    if (ratingMinValue <= each_command[2] <= ratingMaxValue):
                        file.write(
                            table_name + ',' + str(int(each_command[0])) + ',' + str(int(each_command[1])) + ',' + str(
                                float(each_command[2])) + '\n')

        file.close()
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
    #pass #Remove this once you are done with implementation

def PointQuery_Write_Function(file, num_partitions, table_prefix, value, cursor):
    for idx in range(num_partitions):
        table_name = "{}{}".format(table_prefix, idx)
        cursor.execute("SELECT * from {}".format(table_name))
        res_table = cursor.fetchall()
        for each_command in res_table:
            if (each_command[2] == value):
                file.write(table_name + ',' + str(int(each_command[0])) + ',' + str(int(each_command[1])) + ',' + str(
                    float(each_command[2])) + '\n')

def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    try:
        cursor = openconnection.cursor()
        file = open(outputPath, "w+")

        cursor.execute(" select * from RoundRobinRatingsMetadata")
        rrobin_partitions = cursor.fetchone()
        PointQuery_Write_Function(file, rrobin_partitions[0], RROBIN_TABLE_PREFIX, ratingValue, cursor)

        cursor.execute("select * from  RangeRatingsMetadata")
        metadata = cursor.fetchall()
        for command in metadata:
            minrating = command[1]
            maxrating = command[2]
            if(ratingValue > maxrating):
                continue
            if(ratingValue < minrating):
                break
            else:
                table_name = "{}{}".format(RANGE_TABLE_PREFIX, command[0])
                cursor.execute("SELECT * from {}".format(table_name))
                res_table = cursor.fetchall()
                for each_command in res_table:
                    if (each_command[2] == ratingValue):
                        file.write(
                            table_name + ',' + str(int(each_command[0])) + ',' + str(int(each_command[1])) + ',' + str(
                                float(each_command[2])) + '\n')
        file.close()
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
    #pass # Remove this once you are done with implementation
