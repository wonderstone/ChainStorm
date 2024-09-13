package mongo

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gopkg.in/yaml.v3"
)

type void struct{}

type Node struct {
	ID         primitive.ObjectID     `bson:"_id,omitempty"`
	Collection string                 `bson:"collection"`
	Name       string                 `bson:"name"`
	Data       map[string]interface{} `bson:"data"`
}

func (v Node) Export() map[string]interface{} {
	return map[string]interface{}{
		"collection": v.Collection,
		"name":       v.Name,
		"data":       v.Data,
	}
}

type Edge struct {
	ID           primitive.ObjectID     `bson:"_id,omitempty"`
	From         primitive.ObjectID     `bson:"from"`
	To           primitive.ObjectID     `bson:"to"`
	Collection   string                 `bson:"collection"`
	Relationship string                 `bson:"relationship"`
	Data         map[string]interface{} `bson:"data"`
}

func (e Edge) Export() map[string]interface{} {
	return map[string]interface{}{
		"from":         e.From,
		"to":           e.To,
		"collection":   e.Collection,
		"relationship": e.Relationship,
		"data":         e.Data,
	}
}



type MongoGraph struct {
	username string
	password string
	server   string
	port     int
	database string

	// use collectionSet to store the created collection names
	collectionSet map[string]void
	
	client *mongo.Client
}


func (mg *MongoGraph) Init(yamlPath string) error {
	var err error
	// recover from panic
	defer recoverFromPanic(&err)

	yamlData, err := os.ReadFile(yamlPath)
	if err != nil {
		return err
	}


	// unmarshal the yaml data into a map
	var data map[string]interface{}
	err = yaml.Unmarshal(yamlData, &data)
	if err != nil {
		return err
	}
	
	// get the yaml data
	mg.username = data["username"].(string)
	mg.password = data["password"].(string)
	mg.server = data["server"].(string)
	mg.port = data["port"].(int)
	mg.database = data["database"].(string)

	mg.collectionSet = make(map[string]void)

	return err
}

// implement the Connect method
func (mg *MongoGraph) Connect() error {
	// build the uri with the username , password , server and port
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d", mg.username, mg.password, mg.server, mg.port)
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}
	mg.client = client

	// get all the collections in the database and assign to the collectionSet
	collections, err := client.Database(mg.database).ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return err
	}

	// create a new collectionSet
	mg.collectionSet = make(map[string]void)
	for _, col := range collections {
		mg.collectionSet[col] = void{}
	}


	return nil
}

// implement the Disconnect method
func (mg *MongoGraph) Disconnect() error {
	err := mg.client.Disconnect(context.TODO())
	if err != nil {
		return err
	}
	return nil
}



// func to check if the collection exists
func (mg *MongoGraph) collectionExists(collection string) bool {
	_, ok := mg.collectionSet[collection]
	return ok
}

// func to create new collection with the name as unique index
func (mg *MongoGraph) createCollection(collection string) error {
	// get the database
	db := mg.client.Database(mg.database)
	// index model
	indexModel := mongo.IndexModel{
		Keys: bson.M{"name": 1},
		Options: options.Index().SetUnique(true),
	}
	// create the collection
	err := db.CreateCollection(context.Background(),collection)

	// create the index
	_, err = db.Collection(collection).Indexes().CreateOne(context.Background(), indexModel)
	if err != nil {
		return err
	}
	// add the collection to the collectionSet
	mg.collectionSet[collection] = void{}
	return nil
}

// implement the AddNode method，
// return the inserted ID and error
func (mg *MongoGraph) AddNode(n Node)  (interface{}, error) {
	var err error
	defer recoverFromPanic(&err)
	// check if the collection exists, if not create the collection
	if !mg.collectionExists(n.Collection) {
		// create the collection
		err = mg.createCollection(n.Collection)
		if err != nil {
			return nil,err
		}
	}

	// right now the collection exists and has name as unique index
	// insert the node
	db := mg.client.Database(mg.database)
	verticesCol := db.Collection(n.Collection)

	// insert the node
	res, err := verticesCol.InsertOne(context.TODO(), n)
	if err != nil {
		return nil,err
	}
	insertedID := res.InsertedID
	return insertedID, nil
}

// implement the GetNode method
// return the node and error
func (mg *MongoGraph) GetNode(id interface{}) (Node, error) {
	var err error
	defer recoverFromPanic(&err)

	// get the database and collection
	db := mg.client.Database(mg.database)

	dt := id.(map[string]interface{})

	colName := dt["Collection"].(string)
	nodeName := dt["Name"].(string)
	verticesCol := db.Collection(colName)

	// find the node
	var node Node
	err = verticesCol.FindOne(context.TODO(), bson.M{"name": nodeName}).Decode(&node)
	if err != nil {
		return Node{}, err
	}
	return node, nil
}

// GetNodeID method
func (mg *MongoGraph) GetNodeID(id interface{}) (interface{}, error) {
	var err error
	defer recoverFromPanic(&err)

	// get the database and collection
	db := mg.client.Database(mg.database)
	dt := id.(map[string]interface{})

	colName := dt["Collection"].(string)
	nodeName := dt["Name"].(string)
	verticesCol := db.Collection(colName)

	// find the node
	var node Node
	err = verticesCol.FindOne(context.TODO(), bson.M{"name": nodeName}).Decode(&node)
	if err != nil {
		return Node{}, err
	}

	return node.ID, nil
}

// // implement the GetEdge method
// func (mg *MongoGraph) GetEdge(id interface{}) (Edge, error) {
// 	var err error
// 	defer recoverFromPanic(&err)

// 	// get the database and collection
// 	db := mg.client.Database(mg.database)
// 	edgesCol := db.Collection("edges")

// 	// find the edge
// 	var edge Edge
// 	err = edgesCol.FindOne(context.TODO(), bson.M{"_id": id}).Decode(&edge)
// 	if err != nil {
// 		return Edge{}, err
// 	}
// 	return edge, nil
// }



func main() {
	// 1. Connect to MongoDB
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO())

	// Ping the MongoDB server
	err = client.Ping(context.TODO(), readpref.Primary())
	if err != nil {
		log.Fatal("Could not connect to MongoDB: ", err)
	}
	fmt.Println("Connected to MongoDB!")

	// 2. Access the database and collections
	db := client.Database("graphdb")
	verticesCol := db.Collection("vertices")
	edgesCol := db.Collection("edges")

	// 3. Define vertices with map[string]interface{} in the Data field
	alice := Node{
		Name: "Alice",
		Data: map[string]interface{}{
			"age":  30,
			"city": "New York",
			"tags": []string{"engineer", "blogger"}, // Array in the flexible data field
		},
	}
	bob := Node{
		Name: "Bob",
		Data: map[string]interface{}{
			"age":  25,
			"city": "San Francisco",
			"hobbies": map[string]interface{}{
				"outdoor": true,
				"sports":  "tennis",
			},
		},
	}

	// 4. Insert the vertices (nodes)
	result1, err := verticesCol.InsertOne(context.TODO(), alice)
	if err != nil {
		log.Fatal(err)
	}
	aliceID := result1.InsertedID.(primitive.ObjectID)

	result2, err := verticesCol.InsertOne(context.TODO(), bob)
	if err != nil {
		log.Fatal(err)
	}
	bobID := result2.InsertedID.(primitive.ObjectID)

	fmt.Printf("Inserted Alice with ID: %v\n", aliceID)
	fmt.Printf("Inserted Bob with ID: %v\n", bobID)

	// 5. Insert an edge (relationship)
	friendEdge := Edge{From: aliceID, To: bobID, Relationship: "friends"}

	_, err = edgesCol.InsertOne(context.TODO(), friendEdge)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserted edge between Alice and Bob")

	// 6. Query the vertices
	fmt.Println("\nQuerying vertices:")
	cursor, err := verticesCol.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		var vertex Node
		err := cursor.Decode(&vertex)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Vertex: %v, Data: %v\n", vertex.Name, vertex.Data)
	}
}
