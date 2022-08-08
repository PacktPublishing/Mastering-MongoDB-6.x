from pymongo import MongoClient
import json

class InitData:
    def __init__(self):
        self.client = MongoClient('localhost', 27017, w='majority')
        self.db = self.client.mongo_bank
        self.accounts = self.db.accounts

        # drop data from accounts collection every time to start from a clean slate
        self.db.drop_collection('accounts')

        init_data = InitData.load_data(self)
        self.insert_data(init_data)

        #alex=100, mary=50
        self.tx_transfer_err('1', '2', 300)
        # alex=100, mary=50
        self.tx_transfer_err('1', '2', 90)
        # alex=10, mary=140

        # alex=70, mary=80
        # self.tx_transfer_err('2', '1', 20)
        # alex=90, mary=60
        # self.tx_transfer_err_ses('2', '1', 200)

    @staticmethod
    def load_data(self):
        ret = []
        with open('init_data.json', 'r') as f:
            for line in f:
                ret.append(json.loads(line))
        return ret

    def insert_data(self, data):
        for document in data:
            # breakpoint()
            collection_name = document['collection']
            account_id = document['account_id']
            account_name = document['account_name']
            account_balance = document['account_balance']

            self.db[collection_name].insert_one({'account_id': account_id, 'name': account_name, 'balance': account_balance})

    # we are updating outside of a tx
    def transfer(self, source_account, target_account, value):
        print(f'transferring {value} Hypnotons from {source_account} to {target_account}')
        with self.client.start_session() as ses:
            ses.start_transaction()
            self.accounts.update_one({'account_id': source_account}, {'$inc': {'balance': value*(-1)} })
            self.accounts.update_one({'account_id': target_account}, {'$inc': {'balance': value} })

            updated_source_balance = self.accounts.find_one({'account_id': source_account})['balance']
            updated_target_balance = self.accounts.find_one({'account_id': target_account})['balance']
            if updated_source_balance < 0 or updated_target_balance < 0:
                ses.abort_transaction()
            else:
                ses.commit_transaction()

    # transfer using a tx
    def tx_transfer(self, source_account, target_account, value):
        print(f'transferring {value} Hypnotons from {source_account} to {target_account}')
        with self.client.start_session() as ses:
            ses.start_transaction()
            self.accounts.update_one({'account_id': source_account}, {'$inc': {'balance': value*(-1)} }, session=ses)
            self.accounts.update_one({'account_id': target_account}, {'$inc': {'balance': value} }, session=ses)
            ses.commit_transaction()

    # validating errors, not using the tx session
    def tx_transfer_err(self, source_account, target_account, value):
        print(f'transferring {value} Hypnotons from {source_account} to {target_account}')
        with self.client.start_session() as ses:
            ses.start_transaction()
            res = self.accounts.update_one({'account_id': source_account}, {'$inc': {'balance': value*(-1)} }, session=ses)
            res2 = self.accounts.update_one({'account_id': target_account}, {'$inc': {'balance': value} }, session=ses)
            error_tx = self.__validate_transfer(source_account, target_account)

            if error_tx['status'] == True:
                print(f"cant transfer {value} Hypnotons from {source_account} ({error_tx['s_bal']}) to {target_account} ({error_tx['t_bal']})")
                ses.abort_transaction()
            else:
                ses.commit_transaction()

    # validating errors, using the tx session
    def tx_transfer_err_ses(self, source_account, target_account, value):
        print(f'transferring {value} Hypnotons from {source_account} to {target_account}')
        with self.client.start_session() as ses:
            ses.start_transaction()
            res = self.accounts.update_one({'account_id': source_account}, {'$inc': {'balance': value * (-1)}},
                                           session=ses)
            res2 = self.accounts.update_one({'account_id': target_account}, {'$inc': {'balance': value}},
                                            session=ses)
            error_tx = self.__validate_transfer_ses(source_account, target_account, ses)

            if error_tx['status'] == True:
                print(f"cant transfer {value} Hypnotons from {source_account} ({error_tx['s_bal']}) to {target_account} ({error_tx['t_bal']})")
                ses.abort_transaction()
            else:
                ses.commit_transaction()

    # we are outside the transaction so we cant see the updated values
    def __validate_transfer(self, source_account, target_account):
        source_balance = self.accounts.find_one({'account_id': source_account})['balance']
        target_balance = self.accounts.find_one({'account_id': target_account})['balance']

        if source_balance < 0 or target_balance < 0:
            return {'status': True, 's_bal': source_balance, 't_bal': target_balance}
        else:
            return {'status': False}


    # we are passing the session value so that we can view the updated values
    def __validate_transfer_ses(self, source_account, target_account, ses):
        source_balance = self.accounts.find_one({'account_id': source_account}, session=ses)['balance']
        target_balance = self.accounts.find_one({'account_id': target_account}, session=ses)['balance']
        if source_balance < 0 or target_balance < 0:
            return {'status': True, 's_bal': source_balance, 't_bal': target_balance}
        else:
            return {'status': False}

def main():
    InitData()

if __name__ == '__main__':
    main()