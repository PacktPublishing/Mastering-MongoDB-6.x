from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.errors import OperationFailure

class ECommerce:
    def __init__(self):
        self.client = MongoClient('localhost', 27017, w='majority')
        self.db = self.client.mongo_bank

        self.users = self.db['users']
        self.carts = self.db['carts']
        self.payments = self.db['payments']
        self.inventories = self.db['inventories']

        # delete any existing data
        self.db.drop_collection('carts')
        self.db.drop_collection('payments')
        self.db.inventories.remove()

        # insert new data
        self.insert_data()

        alex_order_cart_id = self.add_to_cart(1,101,2)

        barbara_order_cart_id = self.add_to_cart(2,101,4)

        self.place_order(alex_order_cart_id)

        self.place_order(barbara_order_cart_id)

    def insert_data(self):
        self.users.insert_one({'user_id': 1, 'name': 'alex' })
        self.users.insert_one({'user_id': 2, 'name': 'barbara'})

        self.carts.insert_one({'cart_id': 1, 'user_id': 1})
        self.db.carts.insert_one({'cart_id': 2, 'user_id': 2})

        self.db.payments.insert_one({'cart_id': 1, 'name': 'alex', 'item_id': 101, 'status': 'paid'})
        self.db.inventories.insert_one({'item_id': 101, 'description': 'bull bearing', 'price': 100, 'quantity': 5.0})


    def add_to_cart(self, user, item, quantity):
        # find cart for user
        cart_id = self.carts.find_one({'user_id':user})['cart_id']

        self.carts.update_one({'cart_id': cart_id}, {'$inc': {'quantity': quantity}, '$set': { 'item': item} })

        return cart_id

    def place_order(self, cart_id):
            while True:
                try:
                    with self.client.start_session() as ses:
                        ses.start_transaction()
                        cart = self.carts.find_one({'cart_id': cart_id}, session=ses)
                        item_id = cart['item']
                        quantity = cart['quantity']
                        # update payments
                        self.db.payments.insert_one({'cart_id': cart_id, 'item_id': item_id, 'status': 'paid'}, session=ses)
                        # remove item from cart
                        self.db.carts.update_one({'cart_id': cart_id}, {'$inc': {'quantity': quantity * (-1)}}, session=ses)
                        # update inventories
                        self.db.inventories.update_one({'item_id': item_id}, {'$inc': {'quantity': quantity*(-1)}}, session=ses)

                        ses.commit_transaction()
                        break
                except (ConnectionFailure, OperationFailure) as exc:
                    print("Transaction aborted. Caught exception during transaction.")

                    # If transient error, retry the whole transaction
                    if exc.has_error_label("TransientTransactionError"):
                        print("TransientTransactionError, retrying transaction ...")
                        continue
                    elif str(exc) == 'Document failed validation':
                        print("error validating document!")
                        raise
                    else:
                        print("Unknown error during commit ...")
                        raise

def main():
    ECommerce()

if __name__ == '__main__':
    main()