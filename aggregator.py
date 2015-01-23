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

from inspect import isfunction
from collections import defaultdict
from pprint import pprint
from math import log10, floor
from pymongo import MongoClient

#grouping modes
STRING = 2 # group by the subdocument literal string
LENGTH = 1 # group by the length of the subdocument, which is an array
DICT_LENGTH = 3 # group by the length of the subdocument, which is a dictionary

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
    lockPings = getSubDoc(doc, "ping.configLockpings")
    hostName = getSubDoc(doc, "ping.hostInfo.system.hostname")
    lockPingSet = set()
    if not (lockPings and hostName):
        outputDict["N/A"] += 1
        return
    for item in lockPings:
        parts = item["_id"].split(":")
        lockPingSet.add(parts[0].upper() + ":" + parts[1].upper())
    matched = False
    for k, hostSet in outputDict.iteritems():
        if k == "N/A": continue
        if len(hostSet.union(lockPingSet)) < len(hostSet) + len(lockPingSet):
            # lockPingSet and hostSet has overlap, meaning they belong to the same host group
            hostSet.add(hostName.upper())
            matched = True

    if not matched:
        outputDict[hash(hostName)] = set([hostName.upper()])

# list of queries to get a subset of documents
# basic_query = {"gid":"4d63f8b4e528c81a1fd9dc1c"}
basic_query = {}
criteria = [basic_query, {"ping.configLockpings.50":{"$exists": True}}, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query, basic_query]

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
modes = [STRING, STRING, LENGTH, DICT_LENGTH, DICT_LENGTH, STRING, STRING, ops_aggregator, STRING, LENGTH, STRING, LENGTH, moves_uptime]

# TEST COMMANDSi
enableTestCommands = 1
runSelectedAggregator = [13]

# helper methods
def outputCsv(outFileName, contentTuple, groupName, clusterId):
    # writes a 2-element tuple to a csv formatted file
    with open(outFileName + ".csv", "wb") as csvFile:
        writer = csv.writer(csvFile)
        for itemPair in contentTuple:
            writer.writerow([itemPair[0], itemPair[1]])

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

def getClusterId(doc, clusterId)

def main():

    client = MongoClient()

    dbName = "euphonia"
    collName = "pings"
    
    groups = client[dbName][collName].aggregate({"$group": {"_id": "$gid", "size":{"$sum":1}}})

    for groupDict in groups: 
        group = groupDict["_id"]
        groupSize = groupDict["size"]

        # get all the clusters
        for doc in client[dbName][colName].find({"gid":group}, {"ping.configLockpings":1, "ping.hostInfo.system.hostname":1}):
            clusterDict = defaultdict(int)
            cluster_count(doc, clusterDict)

        for i, (criterion, projection, mode) in enumerate(zip(criteria, projections, modes)):
            if enableTestCommands and i not in runSelectedAggregator: 
                continue

            print ""
            print "DB name: ", dbName
            print "collection name: ", collName
            print "projection: ", projection
            print "criterion", criterion
            print "mode ", mode

            outputDict = defaultdict(int)
            
            try: 
                # projection is a string
                hash(projection) 
                fields = {projection: 1}
            except TypeError:
                # projection is an array 
                fields = dict((k,1) for k in projection)
            
            # get hostname to determine which cluster we're on
            fields["ping.hostInfo.system.hostname"] = 1
            
            criterion["gid"] = group

            for doc in client[dbName][collName].find(criterion, fields=fields):
                if isfunction(mode): # custom mode which is a function
                    mode(doc, projection, outputDict)
                else:
                    subdoc = getSubDoc(doc, projection)
                    if not subdoc: 
                        outputDict["N/A"] += 1
                        continue
                if mode == LENGTH:
                    outputDict[len(subdoc)] += 1
                elif mode == DICT_LENGTH:
                    try:
                        outputDict[len(subdoc.keys())] += 1
                    except AttributeError as e:
                        print >> sys.stderr, e
                        # data is not clean, fall back to list length
                        outputDict[len(subdoc)] += 1
                elif mode == STRING:
                    outputDict[subdoc] += 1

            sortedOuptutDict = sorted(outputDict.items(), key=operator.itemgetter(1), reverse=True)
            outputCsv(str(projection) + str(time.time()), sortedOuptutDict)
            pprint(sortedOuptutDict)

if __name__ == '__main__':
    main()
