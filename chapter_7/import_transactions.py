#!/usr/bin/env python
import json, pymongo
from pymongo import MongoClient
import random
import datetime
import locale

""" a script to append timestamps to our transactions and insert them to the transactions collection of mongo_book database in the default localhost:27017 MongoDB database"""

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

client = MongoClient()
db = client.mongo_book
collection = db.transactions

def random_date(start, end):
    """Generate a random datetime between `start` and `end`"""
    return start + datetime.timedelta(
        # Get a random amount of seconds between `start` and `end`
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

"""adding random tags in our documents, simulating our classification of a transaction as scam or ico"""
def random_tags():
    ret = []
    coin = random.randint(1, 2)
    if coin == 1:
        ret.append("scam")
    coin = random.randint(1,2)
    if coin == 1:
        ret.append("ico")
    return ret

transactions = []
for line in open('ethereum-data/ethereum-transactions.json', 'r'):
    transactions.append(json.loads(line))
for transaction in range(0, 24):
    for group in range(0, 24):
        tx = transactions[transaction]['result']['extractorData']['data'][0]['group'][group]
        random_datetime = random_date(datetime.datetime(year=2017, month=1, day=24), datetime.datetime(year=2017, month=6, day=24))
        transaction_doc = {
            'from': tx['From'][0]['text'],
            'to': tx['To'][0]['href'][29:],
            'txhash': tx['Tx Hash'][0]['text'],
            'txfee': float(tx['Txfee'][0]['text']),
            'value': float(tx['Value'][0]['text'].split()[0]),
            'block': int(tx['Block'][0]['text']),
            'timestamp': random_datetime,
            'tags': random_tags()
        }
        transaction_id = collection.insert_one(transaction_doc).inserted_id
        print(transaction_id)
