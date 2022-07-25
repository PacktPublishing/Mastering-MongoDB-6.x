require 'mongo'
require 'byebug'

class ECommerce
  def initialize
    @client = Mongo::Client.new([ '127.0.0.1:27017' ], database: :mongo_bank)
    db = @client.database

    @users = db[:users]
    @carts = db[:carts]
    @payments = db[:payments]
    @inventories = db[:inventories]

    # drop any existing data
    @users.drop
    @carts.drop
    @payments.drop
    @inventories.delete_many

    # insert data
    @users.insert_one({ "user_id": 1, "name": "alex" })
    @users.insert_one({ "user_id": 2, "name": "barbara" })
    @carts.insert_one({ "cart_id": 1, "user_id": 1 })
    @carts.insert_one({ "cart_id": 2, "user_id": 2 })
    @payments.insert_one({"cart_id": 1, "name": "alex", "item_id": 101, "status": "paid" })
    @inventories.insert_one({"item_id": 101, "description": "bull bearing", "price": 100, "quantity": 5 })

    alex_order_cart_id = add_to_cart(1, 101, 2)

    barbara_order_cart_id = add_to_cart(2, 101, 4)

    place_order(alex_order_cart_id)

    place_order(barbara_order_cart_id)
  end

  def add_to_cart(user, item, quantity)
    session = @client.start_session
    session.start_transaction
    cart_id = @users.find({ "user_id": user}).first['user_id']
    @carts.update_one({"cart_id": cart_id}, {'$inc': { 'quantity': quantity }, '$set': { 'item': item } }, session: session)
    session.commit_transaction
    cart_id
  end

  def place_order(cart_id)
    session = @client.start_session
    session.start_transaction
    cart = @carts.find({'cart_id': cart_id}, session: session).first
    item_id = cart['item']
    quantity = cart['quantity']
    @payments.insert_one({'cart_id': cart_id, 'item_id': item_id, 'status': 'paid'}, session: session)
    @carts.update_one({'cart_id': cart_id}, {'$inc': {'quantity': quantity * (-1)}}, session: session)
    @inventories.update_one({'item_id': item_id}, {'$inc': {'quantity': quantity*(-1)}}, session: session)

    quantity = @inventories.find({'item_id': item_id}, session: session).first['quantity']

    if quantity < 0
      session.abort_transaction
    else
      session.commit_transaction
    end
  end
end

ECommerce.new