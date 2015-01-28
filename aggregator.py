#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file contains a script that aggregates data from the MMS 'Ping' dataset and outputs the result to a CSV file 

import signal
import sys
import json
import os
import operator
import csv
import time
import re

from inspect import isfunction
from collections import defaultdict
from pprint import pprint
from math import log10, floor
from pymongo import MongoClient

#grouping modes
STRING = 2 # group by the subdocument literal string
LENGTH = 1 # group by the length of the subdocument, which is an array
DICT_LENGTH = 3 # group by the length of the subdocument, which is a dictionary

# list of queries to get a subset of documents
# basic_query = {"gid":"4d63f8b4e528c81a1fd9dc1c"}
basic_query = {}

# TEST COMMANDSi
enableTestCommands = 1
# there are two ways of analyzing this data
# 1.  is to use each host as a unit of analysis, in which case each host is equally weighted, 
#     and contributes to a weight of 1 in the final tally
# 2.  is to use each MongoDB deployment as a unit of analysis, in which case each deployment is 
#     equally weighted, but each deployment may have multiple values of a certain property, e.g. mongod version
#     so the weight is 1/[the number of different values of the property in the deployment]
#     e.g. if a deployment has both v2.4 and v2.6, each contributes to 0.5, whereas if only 2.6 exists
#     it contributes 1 to the final tally
aggregateByCluster = 1
runSelectedAggregator = [1]

# Custom Aggregators
def ops_aggregator(doc, projection, outputDict):
    counterDoc = getSubDoc(doc, projection[0])
    numCores = getSubDoc(doc, projection[1])
    if counterDoc and numCores:
        roundInts = -int(floor(log10(counterDoc / float(numCores))))+0
        if roundInts >= 0: roundInts = -1
        outputDict[round(counterDoc / numCores, roundInts)] += 1

def moves_uptime(doc, projection, outputDict):
    moves = getSubDoc(doc, projection[0])
    uptime = getSubDoc(doc, projection[1])
    if moves and uptime:
        roundInts = -int(floor(log10(moves / float(uptime))))+3
        if roundInts >= 0: roundInts = -1
        outputDict[round(moves / uptime, roundInts)] += 1


def cluster_count(doc, outputDict):
    # group documents into clusters
    lockPings = getSubDoc(doc, "ping.configLockpings")
    hostName = getSubDoc(doc, "ping.hostInfo.system.hostname")
    lockPingSet = set()
    if not (lockPings and hostName):
        outputDict["N/A"] += 1
        return
    for item in lockPings:
        parts = item["_id"].split(":")
        lockPingSet.add(parts[0].upper())
    matched = False

    hostName = hostName.split(":")[0].upper()

    for k, hostSet in outputDict.iteritems():
        if k == "N/A": continue
        if len(hostSet.union(lockPingSet)) < len(hostSet) + len(lockPingSet):
            # lockPingSet and hostSet has overlap, meaning they belong to the same host group
            hostSet.add(hostName)
            matched = True

    if not matched:
        outputDict[hash(hostName)] = set([hostName])


# helper methods
def outputCsv(outFileName, contentDict, mode):
    # writes a 2-element tuple to a csv formatted file
    with open(outFileName + ".tsv", "wb") as csvFile:
        writer = csv.writer(csvFile, delimiter="\t")

        #get all the headers
        allKeys = set()
        for clusterDict in contentDict.values():
            allKeys = allKeys.union(set(clusterDict.keys()))

        allKeys = list(allKeys)
        values = [0 for i in allKeys]
        for clusterHash, clusterDict in contentDict.iteritems():
            for k, v in clusterDict.iteritems():
                if aggregateByCluster:
                    values[allKeys.index(k)] += 1.0/len(clusterDict)
                else:
                    values[allKeys.index(k)] += v

        resultDict = dict(zip(allKeys, values))
        sortedResult = sorted(resultDict.items(), key=operator.itemgetter(1), reverse=True)

        writer.writerow([x[0] for x in sortedResult])
        writer.writerow([x[1] for x in sortedResult])
            
        return

def getSubDoc(doc, projection):
    subdoc = doc
    subdocTree = projection.split(".")
    try:
        # iterate through the subdocuments
        for key in subdocTree:
            subdoc = subdoc[key]
    except Exception:
        print >> sys.stderr, "document does not have the field " + projection
        return None 
    return subdoc

def getClusterId(doc, clusterId):
    return

criteria = [basic_query, {"ping.configLockpings.20":{"$exists": True}}, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query]

# the subdocument to group by
projections = [
    "ping.hostInfo.os.name", 
    "ping.hostInfo.os.name", 
    "ping.shards", 
    "ping.configCollections", 
    "ping.configLockpings", 
    "ping.hostInfo.system.numCores", 
    "ping.buildInfo.version", 
    ["ping.serverStatus.opcounters.getmore","ping.hostInfo.system.numCores"], 
    "ping.hostInfo.system.hostname", 
    "ping.mongoses", 
    "ping.startupWarnings.log.ok", 
    "ping.replStatus.members", 
    ["ping.serverStatus.metrics.record.moves","ping.serverStatus.uptime"]
]

# grouping mode
modes = [STRING, STRING, STRING, DICT_LENGTH, DICT_LENGTH, STRING, STRING, ops_aggregator, STRING, LENGTH, STRING, LENGTH, moves_uptime]

def main():

    client = MongoClient()

    dbName = "euphonia"
    collName = "pings"
    
    groups = client[dbName][collName].aggregate({"$group": {"_id": "$gid", "size":{"$sum":1}}})

    clusterDict = defaultdict(int)

    for groupDict in groups["result"]:
        group = groupDict["_id"]
        groupSize = groupDict["size"]
        
        # get all the clusters
        for doc in client[dbName][collName].find({"gid":group}, {"ping.configLockpings":1, "ping.hostInfo.system.hostname":1}):
            cluster_count(doc, clusterDict)

    #dictionary of clusters, each one is a dictionary of the count
    outputDict = defaultdict(lambda: defaultdict(int))

    for i, (criterion, projection, mode) in enumerate(zip(criteria, projections, modes)):
        if enableTestCommands and i not in runSelectedAggregator: 
            continue
        for clusterHash, clusterSet in clusterDict.iteritems():

            #if there is no cluster information in the data, there's a 0 instead of a clusterSet
            if type(clusterSet) == int: continue 
            
            for hostName in clusterSet:
                try: 
                    # projection is a string
                    hash(projection) 
                    fields = {projection: 1}
                except TypeError:
                    # projection is an array 
                    fields = dict((k,1) for k in projection)
                # get hostname to determine which cluster we're on
                fields["ping.hostInfo.system.hostname"] = 1
                
                regx = re.compile(hostName, re.IGNORECASE)

                criterion["ping.hostInfo.system.hostname"] = {"$regex": regx}

                # if there are more than one mongods running on the same host, only one is returned
                # I'm not sure if there is a scenarios where we would like to consider more than one instances of a 
                doc = client[dbName][collName].find_one(criterion, fields=fields)


                if isfunction(mode): # custom mode which is a function
                    mode(doc, projection, outputDict)
                else:
                    subdoc = getSubDoc(doc, projection)
                    if not subdoc: 
                        outputDict[clusterHash]["N/A"] += 1
                        continue
                
                if mode == DICT_LENGTH:
                    try:
                        outputDict[clusterHash][len(subdoc.keys())] += 1
                    except AttributeError as e:
                        print >> sys.stderr, e
                        # data is not clean, fall back to list length
                        outputDict[clusterHash][len(subdoc)] += 1
                elif mode == STRING:
                    outputDict[clusterHash][subdoc] += 1
                elif mode == LENGTH:
                    outputDict[clusterHash][len(subdoc)] += 1
        outputCsv(str(projection) + str(time.time()), outputDict, mode)

if __name__ == '__main__':
    main()
