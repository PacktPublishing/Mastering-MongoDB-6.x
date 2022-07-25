from pymongo import MongoClient
from pprint import pprint

class MongoExamples:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        db = client.mongo_book
        self.books = db.books

    # find all books
    def find_all(self):
        result = list(self.books.find())

        pprint(result)

    # insert document
    def insert_book(self):
        book = {
            'isbn': '301',
            'name': 'Python and MongoDB',
            'price': 60
        }

        insert_result = self.books.insert_one(book)

        pprint(insert_result)

    # find book by top level attribute
    def find_book_by_name(self):
        result = list(self.books.find({"name": "Mastering MongoDB"}))

        pprint(result)

    # find a book by embedded attribute
    def find_book_by_embedded_attribute(self):
        result = list(self.books.find({"meta.authors": {"$regex": "aLEx", "$options": "i"}}))

        pprint(result)


    # logical AND in query
    def find_book_logical_and(self):
        result = list(self.books.find({"name": "Mastering MongoDB", "isbn": "101"}))

        pprint(result)

    # logical OR in query
    def find_book_logical_or(self):
        result = list(self.books.find({"$or": [{"isbn": "101"}, {"isbn": "102"}]}))

        pprint(result)


    # combine AND and OR
    def find_book_both_logical_and_or(self):
        result = list(self.books.find({"$or": [{"$and": [{"name": "Mastering MongoDB", "isbn": "101"}]}, {"$and": [{"name": "MongoDB in 7 years", "isbn": "102"}]}]}))

        pprint(result)


    # comparison operators
    def find_comparison_operator(self):
        result = list(self.books.find({"price": { "$gt":50}}))

        pprint(result)


    # update book
    def update_book(self):
        result = self.books.update_one({"isbn": "101"}, {"$set": {"price": 100}}, upsert=False)

        print(result.matched_count)
        print(result.modified_count)

def main():
    MongoExamples().find_all()
    MongoExamples().insert_book()
    MongoExamples().find_book_by_name()
    MongoExamples().find_book_by_embedded_attribute()
    MongoExamples().find_book_logical_and()
    MongoExamples().find_book_logical_or()
    MongoExamples().find_book_both_logical_and_or()
    MongoExamples().find_comparison_operator()
    MongoExamples().update_book()


if __name__ == '__main__':
    main()


