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

from collections import defaultdict
from pprint import pprint
from math import log10, floor
from pymongo import MongoClient

#grouping modes
STRING = 2 # group by the subdocument literal string
LENGTH = 1 # group by the length of the subdocument, which is an array
DICT_LENGTH = 3 # group by the length of the subdocument, which is a dictionary

# Custom Aggregators
def ops_aggregator(doc, projection):
    counterDoc = getSubDoc(doc, projection[0])
    numCores = getSubDoc(doc, projection[1])
    if counterDoc and numCores:
        print counterDoc, numCores
        roundInts = -int(floor(log10(counterDoc / float(numCores))))+0
        if roundInts >= 0: roundInts = -1
        print roundInts
        return round(counterDoc / numCores, roundInts)
    return 0

# list of queries to get a subset of documents
criteria = [{}, {"ping.configLockpings.50":{"$exists": True}}, {}, {}, {}, {}, {}, {}, {}]

# the subdocument to group by
projections = ["ping.hostInfo.os.name", "ping.hostInfo.os.name", "ping.shards", "ping.configCollections", "ping.configLockpings", "ping.hostInfo.system.numCores", "ping.buildInfo.version", ["ping.serverStatus.opcounters.getmore","ping.hostInfo.system.numCores"], "ping.hostInfo.system.hostname"]

# grouping mode
modes = [STRING, STRING, LENGTH, DICT_LENGTH, DICT_LENGTH, STRING, STRING, ops_aggregator, STRING]

# TEST COMMANDS
enableTestCommands = 1
runSelectedAggregator = [8]

def outputCsv(outFileName, contentTuple):
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
        print "document does not have the field " + projection
        return None 
    return subdoc


def main():

    client = MongoClient()

    dbName = "euphonia"
    collName = "pings"
    
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
            hash(projection) 
            fields = {projection: 1}
        except TypeError:
            fields = dict((k,1) for k in projection)

        for doc in client[dbName][collName].find(criterion, fields=fields):
            try: # see if it's a custom mode
                subdoc = mode(doc, projection)
            except TypeError: # if not, rever to using one of the standard modes
                subdoc = getSubDoc(doc, projection)
                if not subdoc: continue
            if mode == LENGTH:
                outputDict[len(subdoc)] += 1
            elif mode == DICT_LENGTH:
                try:
                    outputDict[len(subdoc.keys())] += 1
                    if len(subdoc.keys()) == 24: pprint(doc)
                except AttributeError:
                    # data is not clean, fall back to list length
                    outputDict[len(subdoc)] += 1
            else:
                outputDict[subdoc] += 1

        sortedOuptutDict = sorted(outputDict.items(), key=operator.itemgetter(1), reverse=True)
        outputCsv(str(projection) + str(time.time()), sortedOuptutDict)
        pprint(sortedOuptutDict)

if __name__ == '__main__':
    main()
