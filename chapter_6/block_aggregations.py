# coding: utf-8


from pymongo import MongoClient
from bson.son import SON
import pprint

class BlockAggregation:
    def __init__(self):
        client = MongoClient()
        db = client.mongo_book
        self.collection = db.blocks

    def average_number_transactions_total_block(self):
        pipeline = [
            {"$group": {"_id": "average_transactions_per_block", "count": {"$avg": "$number_transactions"}}},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def average_number_transactions_internal_block(self):
        pipeline = [
            {"$group": {"_id": "average_transactions_internal_per_block", "count": {"$avg": "$number_internal_transactions"}}},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def average_gas_block(self):
        pipeline = [
            {"$group": {"_id": "average_gas_used_per_block",
                        "count": {"$avg": "$gas_used"}}},
        ]
        result = self.collection.aggregate(pipeline)
        for res in result:
            print(res)

    def average_difficulty_block(self):
        pipeline = [
            {"$group": {"_id": "average_difficulty_per_block",
                        "count": {"$avg": "$difficulty"}, "stddev": {"$stdDevPop": "$difficulty"}}},
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


def main():
    # my commands
    # 1. Average number of transactions per block, both in total and also in contract internal transactions
    BlockAggregation().average_number_transactions_total_block()
    BlockAggregation().average_number_transactions_internal_block()

    # 2. Average gas used per block
    BlockAggregation().average_gas_block()

    # 3. Average gas used per transaction in a block, is there a “window of opportunity” to submit my smart contract in a block?

    # 4. Average difficulty per block and how it deviates
    BlockAggregation().average_difficulty_block()

    # 5. top addresses to
    BlockAggregation().top_ten_addresses_to()

    # 6. Average value per transaction
    BlockAggregation().average_value_per_transaction()

    # 7. Average fee per transaction
    BlockAggregation().average_fee_per_transaction()

    # 8. Most Active hour of day (1:Monday, 7: Sunday)
    BlockAggregation().active_hour_of_day_transactions()
    BlockAggregation().active_hour_of_day_values()

    # 9. Most active day of week (1:Monday, 7: Sunday)
    BlockAggregation().active_day_of_week_transactions()
    BlockAggregation().active_day_of_week_values()


if __name__ == '__main__':
    main()
