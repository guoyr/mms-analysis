import signal
import sys
import json
import os
import operator
import csv

from collections import defaultdict
from pprint import pprint

from pymongo import MongoClient

STRING = 2
LENGTH = 1

#find_query = {"ping.configLockpings.50":{"$exists": True} }
find_query = {}
hist_dict = {}

if os.path.isfile(".agg_hist"):
    hist_dict = json.load(open(".agg_hist", "r"))


def save_hist():
    json.dump(hist_dict, open(".agg_hist", "w"))


def signal_handler(signal, frame):
    print("saving command history...")
    save_hist()
    print("\nexiting...")
    sys.exit(0)


def get_default(fieldName, defaultValue):
    if fieldName in hist_dict:
        return hist_dict[fieldName]
    else:
        return defaultValue


def get_input():

    dbName = raw_input("name of DB: ") or get_default("dbName", "euphonia")
    collName = raw_input("name of collection: ") or get_default("collName", "pings")
    subdocName = raw_input("name of sub-document: ") or get_default("subdocName", "ping")
    mode = raw_input("enter the mode (1 for length or 2 for string):") or 2

    hist_dict["dbName"] = dbName
    hist_dict["collName"] = collName
    hist_dict["subdocName"] = subdocName

    return dbName, collName, subdocName, mode


def outputCsv(outFileName, contentTuple):
    with open(outFileName + ".csv", "wb") as csvFile:
        writer = csv.writer(csvFile)
        for itemPair in contentTuple:
            writer.writerow([itemPair[0], itemPair[1]])


def main():

    client = MongoClient()
    signal.signal(signal.SIGINT, signal_handler)
    dbName, collName, subdocName, mode = get_input()
    save_hist()

    print "DB name: ", dbName
    print "collection name: ", collName
    print "subdocument name: ", subdocName

    outputDict = defaultdict(int)

    for doc in client[dbName][collName].find(find_query, fields={subdocName: 1}):
        subdocTree = subdocName.split(".")
        subdoc = doc
        try:
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
    outputCsv("output", sortedOuptutDict)
    pprint(sortedOuptutDict)

if __name__ == '__main__':
    main()