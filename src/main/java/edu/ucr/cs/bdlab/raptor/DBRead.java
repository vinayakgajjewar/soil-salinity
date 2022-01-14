// DBRead.java
// singleton class to handle MongoDB operations

package edu.ucr.cs.bdlab.raptor;

import com.mongodb.client.*;
import com.mongodb.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

class DBRead {

    // single instance
    private static DBRead singleInstance = null;

    // MongoDB client
    //private static MongoClient mongoClient;

    // private constructor
    private DBRead() {

        // try connecting to the MongoDB cluster
        // mongodb.uri is a system variable that needs to be set when you run mvn compile exec:java
        // something like this:
        // mvn compile exec:java -Dexec.mainClass="com.mongodb.quickstart.Read" -Dmongodb.uri="<my secret uri>"
        /* try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            System.out.println("----connected to MongoDB cluster successfully");
            MongoDatabase database = mongoClient.getDatabase("raptor_db");
        MongoCollection<Document> vectorFilePaths = database.getCollection("vector_file_paths");

        // find one document
        Document record = vectorFilePaths.find(new Document("filetype", "rtree")).first();
        System.out.println("sample record: " + record.toJson());
        System.out.println("----reading");
        }
        if (mongoClient == null) {
            System.out.println("mongoClient is null");
        } */
        System.out.println("----reading");
    }

    public static void read() {

        /* MongoDatabase database = mongoClient.getDatabase("raptor_db");
        MongoCollection<Document> vectorFilePaths = database.getCollection("vector_file_paths"); */

        // find one document
        /* Document record = vectorFilePaths.find(new Document("filetype", "rtree")).first();
        System.out.println("sample record: " + record.toJson());
        System.out.println("----reading"); */

        ConnectionString connectionString = new ConnectionString("mongodb://vinayakgajjewar:examplepasswd@freecluster.ts1sn.mongodb.net/test?retryWrites=true&w=majority");

        //ConnectionString connectionString = new ConnectionString("mongodb+srv://vinayakgajjewar:examplepasswd@freecluster.ts1sn.mongodb.net/test?retryWrites=true&w=majority");
        MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(connectionString)
        .build();
        MongoClient mongoClient = MongoClients.create(settings);
        if (mongoClient == null) {
            System.out.println("mongoClient is null");
        }
        List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
        databases.forEach(db -> System.out.println(db.toJson()));
        /* MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> vectorFilePaths = database.getCollection("vector_file_paths");
        Document record = vectorFilePaths.find(new Document("filetype", "rtree")).first();
        System.out.println("sample record: " + record.toJson()); */
    }

    // get or create instance of DBRead class
    public static DBRead getInstance() {
        if (singleInstance == null) {
            singleInstance = new DBRead();
        }
        return singleInstance;
    }
}