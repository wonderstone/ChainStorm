package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
//
	fmt.Println("Hello, World!")
	username := "adminUser"
	password := "adminPassword"
	server := "localhost"
	port := 27017

	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/", username, password, server, port)
	fmt.Println("URI: ", uri)

	// connect to MongoDB
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	defer client.Disconnect(context.TODO())

	// ping the MongoDB server
	err = client.Ping(context.TODO(), readpref.Primary())
	if err != nil {
		log.Fatal("Could not connect to MongoDB: ", err)
	}

	fmt.Println("Connected to MongoDB!")

	// 2. Create a database and collection
	db := client.Database("mydb")
	col := db.Collection("mycollection")

	// 3. Insert a document


	_, err = col.InsertOne(context.TODO(), map[string]string{"name": "Alice"})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Document inserted successfully!")

	// 4. Query the document
	var result map[string]interface{}
	err = col.FindOne(context.TODO(), map[string]string{"name": "Alice"}).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Document found: ", result)

	
}