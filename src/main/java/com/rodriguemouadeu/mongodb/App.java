package com.rodriguemouadeu.mongodb;


import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class App  {
    public static void main( String[] args ) {

        MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(50).build();
        MongoClient client = new MongoClient("localhost", options);
        MongoDatabase database = client.getDatabase("students");
        MongoCollection<Document> collection = database.getCollection("grades");


        // Hint/spoiler: If you select homework grade-documents, sort by student and then by score, you can iterate through and find the
        // lowest score for each student by noticing a change in student id. As you notice that change of student_id, remove the document.

        FindIterable<Document> results = collection.find(new Document("type", "homework")).sort(new BasicDBObject("student_id", 1).append("score", 1));
        System.out.println("Number of grades before processing : " + collection.count());

        MongoCursor<Document> cursor = results.iterator();
        List<Integer> studentIds = new ArrayList<Integer>();
        int counter = 0;

        while(cursor.hasNext()){
            Document currentDoc = cursor.next();
            int studentId = currentDoc.getInteger("student_id");
            if(!studentIds.contains(studentId)){
                studentIds.add(studentId);
                counter++;
                collection.deleteOne(currentDoc);
            }
        }
        System.out.println("Number of students removed : " + counter);
    }
}