from pymongo import MongoClient
from bson.son import SON
import pprint
import datetime

class TransactionAggregation:
    def __init__(self):
        client = MongoClient()
        db = client.mongo_book
        self.collection = db.transactions

    def top_ten_addresses_from(self):
        pipeline = [
            {"$group": {"_id": "$from", "count": {"$sum": 1}}},
            {"$sort": SON([("count", -1)])},
            {"$limit": 10},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def top_ten_addresses_to(self):
        pipeline = [
            {"$group": {"_id": "$to", "count": {"$sum": 1}}},
            {"$sort": SON([("count", -1)])},
            {"$limit": 10},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def average_value_per_transaction(self):
        pipeline = [
            {"$group": {"_id": "value", "averageValues": {"$avg": "$value"}, "stdDevValues": {"$stdDevPop": "$value"}}},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def average_fee_per_transaction(self):
        pipeline = [
            {"$group": {"_id": "value", "averageFees": {"$avg": "$txfee"}, "stdDevValues": {"$stdDevPop": "$txfee"}}},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def active_hour_of_day_transactions(self):
        pipeline = [
            {"$group": {"_id": {"$hour": "$timestamp"}, "transactions": {"$sum": 1}}},
            {"$sort": SON([("transactions", -1)])},
            {"$limit": 1},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def active_hour_of_day_values(self):
        pipeline = [
            {"$group": {"_id": {"$hour": "$timestamp"}, "transaction_values": {"$sum": "$value"}}},
            {"$sort": SON([("transactions", -1)])},
            {"$limit": 1},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def active_day_of_week_transactions(self):
        pipeline = [
            {"$group": {"_id": {"$dayOfWeek": "$timestamp"}, "transactions": {"$sum": 1}}},
            {"$sort": SON([("transactions", -1)])},
            {"$limit": 1},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def active_day_of_week_values(self):
        pipeline = [
            {"$group": {"_id": {"$dayOfWeek": "$timestamp"}, "transaction_values": {"$sum": "$value"}}},
            {"$sort": SON([("transactions", -1)])},
            {"$limit": 1},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def ni_path_length(self):
        pipeline = [

        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def scam_or_ico_aggregation(self):
        pipeline = [
            {"$match": {"timestamp": {"$gte": datetime.datetime(2017,6,1), "$lte": datetime.datetime(2017,7,1)}}},
            {"$project": {
                "to": 1,
                "txhash": 1,
                "from": 1,
                "block": 1,
                "txfee": 1,
                "tags": 1,
                "value": 1,
                "report_period": "June 2017",
                "_id": 0,
                }

            },
            {"$unwind": "$tags"},
            {"$out": "scam_ico_documents"}
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def scam_add_information(self):
        client = MongoClient()
        db = client.mongo_book
        scam_collection = db.scam_ico_documents
        pipeline = [
            {"$lookup": {"from": "scam_details", "localField": "from", "foreignField": "scam_address", "as": "scam_details"}},
            {"$match": {"scam_details": { "$ne": [] }}},
            {"$out": "scam_ico_documents_extended"}
        ]
        result = scam_collection.aggregate(pipeline)
        for res in result:
            print(res)

TransactionAggregation().scam_or_ico_aggregation()
TransactionAggregation().scam_add_information()
TransactionAggregation().top_ten_addresses_from()
TransactionAggregation().top_ten_addresses_to()
TransactionAggregation().average_value_per_transaction()
TransactionAggregation().average_fee_per_transaction()
TransactionAggregation().active_hour_of_day_transactions()
TransactionAggregation().active_hour_of_day_values()
# 1 (Sunday) and 7 (Saturday)
TransactionAggregation().active_day_of_week_transactions()
# 1 (Sunday) and 7 (Saturday)
TransactionAggregation().active_day_of_week_values()