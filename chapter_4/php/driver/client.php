<?php
require 'vendor/autoload.php';

// requires pecl
// mongodb 1.1.9   stable
// 1. sudo pecl install mongodb-1.1.9
// and altering php.ini
// 2. extension=/usr/local/Cellar/php70/7.0.17_9/lib/php/extensions/no-debug-non-zts-20151012/mongodb.so
// 3. composer install
// 4. php client.php
class MongoClient
{
    function __construct()
    {
        // connect
        $client = new MongoDB\Client("mongodb://localhost:27017");

        // select a database
        $db = $client->mongo_book;

        // select a collection (analogous to a relational database's table)
        $this->collection = $db->books;

        $this->manager = new MongoDB\Driver\Manager('mongodb://localhost:27017');
    }

    function find_all() {
        // find everything in the collection
        $cursor = $this->collection->find();

        // iterate through the results
        foreach ($cursor as $document) {
            var_dump($document) . "\n";
        }
    }


    function insert_one() {
    // insert a document
        $document = array( "isbn" => "401", "name" => "MongoDB and PHP" );

        $result = $this->collection->insertOne($document);

        print_r($result);
    }

    function insert_many() {
    // insert many documents
        $documentAlpha = array( "isbn" => "402", "name" => "MongoDB and PHP, 2nd Edition" );
        $documentBeta  = array( "isbn" => "403", "name" => "MongoDB and PHP, revisited" );
        $result = $this->collection->insertMany([$documentAlpha, $documentBeta]);

        print_r($result);

        var_dump($result->getInsertedIds());
        print($result->getInsertedCount());
    }

    function delete_one() {
    // delete one document
        $document = array("isbn" => "403");
        $result = $this->collection->deleteOne($document);
        print_r($result);
    }

    function delete_many() {
    // delete many documents
        $deleteQuery = array( "isbn" => "401");
        $deleteResult = $this->collection->deleteMany($deleteQuery);
        print_r($deleteResult);
        print($deleteResult->getDeletedCount());
    }

    function bulk_insert() {
    // bulk write
        $bulk = new MongoDB\Driver\BulkWrite(array("ordered" => true));
        $bulk->insert(array( "isbn" => "401", "name" => "MongoDB and PHP" ));
        $bulk->insert(array( "isbn" => "402", "name" => "MongoDB and PHP, 2nd Edition" ));
        $bulk->update(array("isbn" => "402"), array('$set' => array("price" => 15)));
        $bulk->insert(array( "isbn" => "403", "name" => "MongoDB and PHP, revisited" ));


        $result = $this->manager->executeBulkWrite('mongo_book.books', $bulk);
        print_r($result);
    }

    function find_regex() {
    // find document with regex
        $cursor = $this->collection->find( array( "name" => new MongoDB\BSON\Regex("mongo", "i") ) );
        foreach ($cursor as $document) {
            var_dump($document) . "\n";
        }
    }

    function find_compare() {
    // find document with comparison
        $cursor = $this->collection->find( array('price' => array('$gte'=> 60) ) );
        foreach ($cursor as $document) {
            var_dump($document) . "\n";
        }
    }

    function find_or_comparison() {
    // find document with OR and comparison queries
        $cursor = $this->collection->find( array( '$or' => array(
                                                     array("price" => array( '$gte' => 60)),
                                                     array("price" => array( '$lte' => 20))
                                            )));
        foreach ($cursor as $document) {
            var_dump($document) . "\n";
        }
    }



}

$mongo_client = new MongoClient();
$mongo_client->find_all();
$mongo_client->insert_one();
$mongo_client->insert_many();
$mongo_client->delete_one();
$mongo_client->delete_many();
$mongo_client->bulk_insert();
$mongo_client->find_regex();
$mongo_client->find_compare();
$mongo_client->find_or_comparison();
?>
