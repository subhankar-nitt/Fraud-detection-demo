package com.example.data

import com.mongodb.client.MongoDatabase
import com.mongodb.client.MongoCollection
import com.mongodb.client.FindIterable

import org.bson.Document
import scala.collection.Iterator


class Data {
  
  def printData(db:MongoDatabase,collectionName : String ){
  val collection = db.getCollection(collectionName)
  val iterDoc: FindIterable[Document]  =collection.find()
  val it = iterDoc.iterator()
  while(it.hasNext()){
    println(it.next())
    }
  }
  
  def insertData(db:MongoDatabase,collectionName:String,doc:Document){
    val collection = db.getCollection(collectionName)
    collection.insertOne(doc)
  }
}