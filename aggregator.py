import signal
import sys
import json
import os
import operator
import csv

from collections import defaultdict
from pprint import pprint

from pymongo import MongoClient

#grouping modes
STRING = 2 # group by the subdocument literal string
LENGTH = 1 # group by the length of the subdocument

# list of queries to get a subset of documents
filterQueries = [{}, {"ping.configLockpings.50":{"$exists": True}}]

# the subdocument to group by
subdocNames = ["ping.hostinfo.os","ping.hostinfo.os"]

# grouping mode
modes = [STRING, STRING]


def outputCsv(outFileName, contentTuple):
    with open(outFileName + ".csv", "wb") as csvFile:
        writer = csv.writer(csvFile)
        for itemPair in contentTuple:
            writer.writerow([itemPair[0], itemPair[1]])


def main():

    client = MongoClient()

    dbName = "euphonia"
    collname = "pings"

    for filterQuery, subdocName, mode in zip(filterQueries, subdocNames, modes):

        print ""
        print "DB name: ", dbName
        print "collection name: ", collName()
        print "subdocument name: ", subdocName
        print "filter query ", filterQuery
        print "mode ", mode

        outputDict = defaultdict(int)

        for doc in client[dbName][collName].find(filterQuery, fields={subdocName: 1}):
            subdocTree = subdocName.split(".")
            subdoc = doc
            try:
                # iterate through the subdocuments
                for key in subdocTree:
                    subdoc = subdoc[key]
            except Exception:
                print "document does not have the field " + subdocName
                continue

            if mode == LENGTH:
                outputDict[len(subdoc)] += 1
            elif mode == STRING:
                outputDict[subdoc] += 1

        sortedOuptutDict = sorted(outputDict.items(), key=operator.itemgetter(1), reverse=True)
        outputCsv(subdocName, sortedOuptutDict)
        pprint(sortedOuptutDict)

if __name__ == '__main__':
    main()