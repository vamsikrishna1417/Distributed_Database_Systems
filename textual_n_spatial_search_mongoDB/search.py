#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import re
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    #query = {'city':re.compile(cityToSearch, re.IGNORECASE)}
    query = {'city': cityToSearch}
    outfile = open(saveLocation1, "w")
    for doc in collection.find(query):
        string = doc['name'].encode('utf-8')+"$"+doc['full_address'].encode('utf-8')+"$"+doc['city'].encode('utf-8')+"$"+doc['state'].encode('utf-8')+"\n"
        outfile.write(string.upper())
    outfile.close()

def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat/2) * math.sin(delta_lat/2) + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lon/2) * math.sin(delta_lon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c
    return distance

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    myLat = float(myLocation[0])
    myLon = float(myLocation[1])
    outfile = open(saveLocation2, "w")
    query = {'categories': {"$all":categoriesToSearch}}
    for doc in collection.find(query):
        destLat = doc['latitude']
        destLon = doc['longitude']
        caldist = DistanceFunction(destLat, destLon, myLat, myLon)
        if caldist <= maxDistance:
            string = doc['name'].encode('utf-8')+'\n'
            outfile.write(string.upper())
    outfile.close()