#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math

def IsCategoryPresent(record, categoriesToSearch):
    key_ = "categories"
    category = record[key_]

    # Convert both the list to upper case
    category = list(map(lambda x: x.upper(), category))

    is_subset =  set(categoriesToSearch).issubset(category)
    return is_subset


def IsWithinDistance(record, myLocation, maxDistance):
    R = 3959.0
    # R = 6371.0
    latitude1 = math.radians(record["latitude"])
    longitude1 = math.radians(record["longitude"])

    latitude2, longitude2 = [math.radians(eval(x)) for x in myLocation]

    delta_lat = latitude2 - latitude1 # delta_phi
    delta_long = longitude2 - longitude1  # delta_lambda

    a = math.sin(delta_lat / 2)**2 + math.cos(latitude1) * math.cos(latitude2) * math.sin(delta_long / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c

    is_within_dist = distance <= maxDistance

    return is_within_dist



# def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
#     import pprint
#     search = {}
#
#     # Hack. Try with given string. If no rec found change
#     search["city"] = cityToSearch
#     all_rec = collection.find_one(search)
#
#     if all_rec == None:
#         search["city"] = cityToSearch[0].upper() + cityToSearch[1:]
#
#     # print(str(search))
#     all_rec = collection.find(search)
#
#     keys = ['name', 'full_address', 'city', 'state']
#     with open(saveLocation1, 'w') as f:
#         for rec in all_rec:
#             rec_string = '$'.join(list(map(lambda x: rec[x], keys)))
#             f.write(rec_string + '\n')
#             print(rec_string)
#
#     a = 20
#     pass

def GetName(record):
    return record["city"].upper()

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):

    # Convert cityToSearch in uppercase
    cityToSearch = cityToSearch.upper()

    # Get sll the records
    all_rec = collection.find()

    # Filter the records based on cityToSearch
    filtered_recs = filter(lambda x: GetName(x) == cityToSearch, all_rec)

    # Define which keys to extract. The order will be maintained using as we are using map
    keys = ['name', 'full_address', 'city', 'state']
    with open(saveLocation1, 'w') as f:
        for rec in filtered_recs:
            rec_string = '$'.join(list(map(lambda x: rec[x], keys)))
            rec_string = rec_string.upper()
            f.write(rec_string + '\n')

    pass


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):

    all_rec = collection.find()

    # convert all the catgories in to uppercase.
    categoriesToSearch = list(map(lambda x: x.upper(), categoriesToSearch))

    # Use 2 filters: 1. categoriesToSearch 2. dist from myLocation

    # Category Filter
    match_category_rec = list(filter(lambda record: IsCategoryPresent(record, categoriesToSearch), all_rec))

    # Distance Filter
    match_location_rec = list(filter(lambda record: IsWithinDistance(record, myLocation, maxDistance), match_category_rec))

    # Get all the names from all the filtered rows
    all_names = [record['name'] for record in match_location_rec]

    # Save it in the file
    with open(saveLocation2, 'w') as f:
        for name in all_names:
            f.write(name.upper()  + '\n')
    pass

