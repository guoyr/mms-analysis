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
CLUSTER_STRING = 4 # same as string, but each cluster counts as 1
CLUSTER_LENGTH = 5
CLUSTER_DICT_LENGTH = 6
# list of queries to get a subset of documents
# basic_query = {"gid":"4d63f8b4e528c81a1fd9dc1c"}
basic_query = {}

# TEST COMMANDSi
enableTestCommands = 1
runSelectedAggregator = [6]

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
    with open(outFileName + ".csv", "wb") as csvFile:
        writer = csv.writer(csvFile, delimiter="\t")
        ifFirstRow = True

        #get all the headers
        allKeys = set()
        for clusterDict in contentDict.values():
            allKeys = allKeys.union(set(clusterDict.keys()))

        allKeys = list(allKeys)
        print allKeys
        if mode in (CLUSTER_STRING, CLUSTER_LENGTH, CLUSTER_DICT_LENGTH):
            values = [0 for i in allKeys]
            for clusterHash, clusterDict in contentDict.iteritems():
                for k, v in clusterDict.iteritems():
                    values[allKeys.index(k)] += 1

            resultDict = dict(zip(allKeys, values))
            sortedResult = sorted(resultDict.items(), key=operator.itemgetter(1), reverse=True)

            writer.writerow([x[0] for x in sortedResult])
            writer.writerow([x[1] for x in sortedResult])
            
            return
        else:
            for clusterHash, clusterDict in contentDict.iteritems():
                keys = sorted(clusterDict.keys())
                writer.writerow(keys)
                values = []
                for k in keys:
                    values.append(clusterDict[k])
                writer.writerow(values)
                ifFirstRow = False

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
modes = [CLUSTER_STRING, CLUSTER_STRING, CLUSTER_LENGTH, CLUSTER_DICT_LENGTH, CLUSTER_DICT_LENGTH, CLUSTER_STRING, CLUSTER_STRING, ops_aggregator, STRING, CLUSTER_LENGTH, STRING, CLUSTER_LENGTH, moves_uptime]

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
                elif mode == CLUSTER_STRING:
                    outputDict[clusterHash][subdoc] = 1
                elif mode == LENGTH:
                    outputDict[clusterHash][len(subdoc)] += 1
                elif mode == CLUSTER_LENGTH:
                    outputDict[clusterHash][len(subdoc)] = 1
                elif mode == CLUSTER_DICT_LENGTH:
                    try:
                        outputDict[clusterHash][len(subdoc.keys())] = 1
                    except AttributeError as e:
                        print >> sys.stderr, e
                        # data is not clean, fall back to list length
                        outputDict[clusterHash][len(subdoc)] = 1

        outputCsv(str(projection) + str(time.time()), outputDict, mode)

if __name__ == '__main__':
    main()
