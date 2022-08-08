#!/usr/bin/env python
import json, pymongo
from pymongo import MongoClient
import random
import datetime
import locale
import math

""" a script to append timestamps to our blocks and insert them to the blocks collection of mongo_book database in the default localhost:27017 MongoDB database"""

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

client = MongoClient()
db = client.mongo_book
collection = db.blocks


def random_date(start, end):
    """Generate a random datetime between `start` and `end`"""
    return start + datetime.timedelta(
        # Get a random amount of seconds between `start` and `end`
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

blocks = []
for line in open('ethereum-data/ethereum-blocks.json', 'r'):
    blocks.append(json.loads(line))


for block in range(0, 24):
        block_data = blocks[block]['result']['extractorData']['data'][0]['group']
        block_height = block_data[0]['height'][0]['text']
        number_tx = block_data[1]['transactions'][0]['text'].split()
        block_hash = block_data[2]['Hash'][0]['text']
        difficulty = block_data[6]['difficulty'][0]['text']
        print(difficulty)
        gas_used = block_data[10]['gas_used'][0]['text']
        timestamp = datetime.datetime.fromtimestamp(math.floor(blocks[block]['result']['timestamp']/1000))
        block_doc = {
            'timestamp': timestamp,
            'block_height': int(block_height),
            'number_transactions': int(number_tx[0]),
            'number_internal_transactions': int(number_tx[3]),
            'difficulty': locale.atoi(difficulty),
            'block_hash': block_hash.encode('ascii', 'ignore'),
            'gas_used': locale.atoi(gas_used),
        }
        block_id = collection.insert_one(block_doc).inserted_id
        print(block_id)



