import pymongo
import csv 
from vimap.db_api import collectors
from vimap.db_api.constrains import *


class MongoLoader:
    def __init__(self):
        pass

    def _update(self):
        pass

    def load_mongo(self, mapped_data, **kwargs):
        print("-" * 40 + "LOADING" + "-" * 40)
        for sample, matched_id in mapped_data:
            if matched_id is None:
                collectors[PLACE_COLLECTION].add_new_place(sample)
        print("Done!")


# if __name__ == '__main__':
#     myclient = pymongo.MongoClient("mongodb+srv://truong:truong@cluster0.plolo.mongodb.net/test")
#     mydb = myclient["open_one"]
#     mycol = mydb["places"]
#
#     file = open('./dataset/school-list.csv', 'r')
#     file_csv = csv.reader(file)
#     header = file_csv.__next__()
#     places_dict = ['id', 'latitude', 'longtitude', 'easting', 'northing', 'address', 'name', 'description', 'place_type', 'metadata']
#     school_dict = ['id', 'head_name', 'telephone', 'website', 'postcode', 'phase']
#     bust_stop_dict = ['id', 'stop_type', 'street', 'locality_name']
#
#     for row in file_csv:
#         insert_dict = dict(zip(places_dict, row))
#         dict = { "name": "John", "address": "Highway 37" }
#         x = mycol.insert_one(insert_dict)
