package mongo

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestInit(t *testing.T) {
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

// test the Connect and Disconnect methods
func TestConnect(t *testing.T) {
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	// ! make sure mongodb container is running
	err = mg.Connect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	fmt.Println(mg.nodeNameCollMap)

	err = mg.Disconnect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	// Check if the connection is closed
	if mg.client != nil && mg.client.Ping(context.TODO(), nil) == nil {
		t.Errorf("Expected connection to be closed, but it is still open")
	}
}

// ~ isNode is only for struct type,
// todo: when dealing with map type as underlying data structure,
// todo: we need to check if the map has the required keys
func TestIsNode(t *testing.T) {
	node := Node{
		Collection: "TestCollection",
		Name:       "TestNode",
		Data:       map[string]interface{}{"key": "value"},
	}

	if !isNode(node) {
		t.Errorf("Expected true, got false")
	}

	nonNode := struct {
		Field string
	}{
		Field: "value",
	}

	if isNode(nonNode) {
		t.Errorf("Expected false, got true")
	}
}

// ~ isEdge is only for struct type,
// todo: when dealing with map type as underlying data structure,
// todo: we need to check if the map has the required keys
func TestIsEdge(t *testing.T) {
	edge := Edge{
		Collection: "TestCollection",
		From:       primitive.NewObjectID(),
		To:         primitive.NewObjectID(),
		Data:       map[string]interface{}{"key": "value"},
	}

	if !isEdge(edge) {
		t.Errorf("Expected true, got false")
	}

	nonEdge := struct {
		Field string
	}{
		Field: "value",
	}

	if isEdge(nonEdge) {
		t.Errorf("Expected false, got true")
	}
}

func TestMapToNode(t *testing.T) {
	validDoc := map[string]interface{}{
		"collection": "TestCollection",
		"name":       "TestNode",
		"data":       map[string]interface{}{"key": "value"},
		"_id":        primitive.NewObjectID(),
	}

	invalidDocMissingKeys := map[string]interface{}{
		"collection": "TestCollection",
		"name":       "TestNode",
	}

	invalidDocWrongTypes := map[string]interface{}{
		"collection": 123,
		"name":       true,
		"data":       "invalidData",
	}

	tests := []struct {
		name    string
		doc     map[string]interface{}
		wantErr bool
	}{
		{"ValidDoc", validDoc, false},
		{"InvalidDocMissingKeys", invalidDocMissingKeys, true},
		{"InvalidDocWrongTypes", invalidDocWrongTypes, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := mapToNode(tt.doc)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapToNode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if node.Collection != tt.doc["collection"] {
					t.Errorf("Expected collection %v, got %v", tt.doc["collection"], node.Collection)
				}
				if node.Name != tt.doc["name"] {
					t.Errorf("Expected name %v, got %v", tt.doc["name"], node.Name)
				}
				if node.Data["key"] != tt.doc["data"].(map[string]interface{})["key"] {
					t.Errorf("Expected data %v, got %v", tt.doc["data"], node.Data)
				}
				if node.ID != tt.doc["_id"] {
					t.Errorf("Expected ID %v, got %v", tt.doc["_id"], node.ID)
				}
			}
		})
	}
}
func TestMapToEdge(t *testing.T) {
	validDoc := map[string]interface{}{
		"from":         primitive.NewObjectID(),
		"to":           primitive.NewObjectID(),
		"collection":   "TestCollection",
		"relationship": "TestRelationship",
		"data":         map[string]interface{}{"key": "value"},
		"_id":          primitive.NewObjectID(),
	}

	invalidDocMissingKeys := map[string]interface{}{
		"from":       primitive.NewObjectID(),
		"collection": "TestCollection",
	}

	invalidDocWrongTypes := map[string]interface{}{
		"from":         "invalidFrom",
		"to":           "invalidTo",
		"collection":   123,
		"relationship": true,
	}

	tests := []struct {
		name    string
		doc     map[string]interface{}
		wantErr bool
	}{
		{"ValidDoc", validDoc, false},
		{"InvalidDocMissingKeys", invalidDocMissingKeys, true},
		{"InvalidDocWrongTypes", invalidDocWrongTypes, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edge, err := mapToEdge(tt.doc)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapToEdge() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if edge.From != tt.doc["from"] {
					t.Errorf("Expected from %v, got %v", tt.doc["from"], edge.From)
				}
				if edge.To != tt.doc["to"] {
					t.Errorf("Expected to %v, got %v", tt.doc["to"], edge.To)
				}
				if edge.Collection != tt.doc["collection"] {
					t.Errorf("Expected collection %v, got %v", tt.doc["collection"], edge.Collection)
				}
				if edge.Relationship != tt.doc["relationship"] {
					t.Errorf("Expected relationship %v, got %v", tt.doc["relationship"], edge.Relationship)
				}
				if edge.Data["key"] != tt.doc["data"].(map[string]interface{})["key"] {
					t.Errorf("Expected data %v, got %v", tt.doc["data"], edge.Data)
				}
				if edge.ID != tt.doc["_id"] {
					t.Errorf("Expected ID %v, got %v", tt.doc["_id"], edge.ID)
				}
			}
		})
	}
}

// AddNode and AddEdge methods are tested together
func TestCRUDNode(t *testing.T) {
	// + init and connect section
	// + Connect also update the graph properties,
	// + say collset, nodeNameCollMap and itemSet
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	err = mg.Connect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	// + drop the collection if it exists
	// + private method collectionExists only checks graph's collSet
	if mg.collectionExists("Company") {
		err2 := mg.dropCollection("Company")
		if err2 != nil {
			t.Errorf("Error: %v", err2)
		}
	}

	// + create 2 nodes
	node01 := Node{
		Collection: "Company",
		Name:       "Google",
		Data: map[string]interface{}{
			"location":  "Mountain View",
			"employees": 1000,
		},
	}
	node02 := Node{
		Collection: "Company",
		Name:       "Facebook",
		Data: map[string]interface{}{
			"location":  "Menlo Park",
			"employees": 2000,
		},
	}
	// + add the first node by passing the pointer
	res01, err01 := mg.AddNode(&node01)

	if err01 != nil {
		t.Errorf("Error: %v", err01)
	}

	// + add the second node by passing the value
	res02, err02 := mg.AddNode(&node02)
	if err02 != nil {
		t.Errorf("Error: %v", err02)
	}

	// - add the first node again should fail
	_, err01_Fail := mg.AddNode(&node01)
	if err01_Fail == nil {
		t.Errorf("Expected error, got nil")
	}

	// + check if the collection exists, if yes drop it
	if mg.collectionExists("Company-Company") {
		err6 := mg.dropCollection("Company-Company")
		if err6 != nil {
			t.Errorf("Error: %v", err6)
		}
	}

	// + create an edge
	edge := Edge{
		Collection: "Company-Company",
		From:       res01.(primitive.ObjectID),
		To:         res02.(primitive.ObjectID),
		Data: map[string]interface{}{
			"relation": "partnership",
		},
	}
	// + add the edge
	res_e, err_e := mg.AddEdge(&edge)
	if err_e != nil {
		t.Errorf("Error: %v", err_e)
	}

	fmt.Println("the inserted ID: ", res_e)

	//+ ReplaceNode(n Node) error
	// given node name, get the node id and create the new node
	// with the same id and replace the old node with the new node

	companyName := "Google"
	node, err := mg.GetNode(companyName)

	if err != nil {
		t.Errorf("Error: %v", err)
	}

	n := node.(*Node)

	newNode := Node{
		ID:         n.ID,
		Collection: "Company",
		Name:       companyName,
		Data: map[string]interface{}{
			"location":  "Mountain Views + Sunnyvale",
			"employees": 2000,
		},
	}

	newNodeAnother := Node{
		ID:         n.ID,
		Collection: "Company",
		Name:       companyName,
		Data: map[string]interface{}{
			"location":  "Mountain Views * Sunnyvale",
			"employees": 5000,
		},
	}

	err3 := mg.ReplaceNode(&newNode)
	if err3 != nil {
		t.Errorf("Error: %v", err3)
	}

	err4 := mg.ReplaceNode(&newNodeAnother)
	if err4 != nil {
		t.Errorf("Error: %v", err4)
	}

	//+ ReplaceEdge(e Edge) error
	// find the edge by  GetInEdges
	edges, errtmp := mg.GetInEdges("Facebook")
	if errtmp != nil {
		t.Errorf("Error: %v", errtmp)
	}

	// get the first edge
	edge1 := edges[0]
	e := edge1.(*Edge)
	// create a new edge with the same id
	newEdge := Edge{
		ID:           e.ID,
		Collection:   "Company-Company",
		From:         e.From,
		To:           e.To,
		Relationship: "partnership +",

		Data: map[string]interface{}{
			"relation": "BBB +",
		},
	}

	err5 := mg.ReplaceEdge(&newEdge)
	if err5 != nil {
		t.Errorf("Error: %v", err5)
	}

	// change some data
	newEdge.Data["relation"] = "CCC +"
	err6 := mg.ReplaceEdge(&newEdge)
	if err6 != nil {
		t.Errorf("Error: %v", err6)
	}

	//+ UpdateNode(n Node) error
	//! node collection should not be changed, or it will be a new node
	newNode.Name = "AliBaba"
	newNode.Data["location"] = "Hangzhou"
	err7 := mg.UpdateNode(&newNode)
	if err7 != nil {
		t.Errorf("Error: %v", err7)
	}

	//+ UpdateEdge(e Edge) error
	//! edge collection should not be changed, or it will be a new edge
	newEdge.Relationship = "partnership ++++++++"
	newEdge.Data["relation"] = "CCC +"
	err8 := mg.UpdateEdge(&newEdge)
	if err8 != nil {
		t.Errorf("Error: %v", err8)
	}

	//+ MergeNode(n Node) error

	newNode.Data["BB location"] = "Hangzhou + Shanghai"
	err9 := mg.MergeNode(&newNode)
	if err9 != nil {
		t.Errorf("Error: %v", err9)
	}

	//+ MergeEdge(e Edge) error
	newEdge.Data["BB relation"] = "CCC ++++++++"
	err10 := mg.MergeEdge(&newEdge)
	if err10 != nil {
		t.Errorf("Error: %v", err10)
	}

	// + Query operations
	// GetItemByID(id interface{}) (interface{}, error)
	item, errGet := mg.GetItemByID(newEdge.ID)

	if errGet != nil {
		t.Errorf("Error: %v", errGet)
	}

	fmt.Println("GetItemByID: ", item)

	// GetNode(name interface{}) (Node, error)
	node, errGetNode := mg.GetNode("AliBaba")
	if errGetNode != nil {
		t.Errorf("Error: %v", errGetNode)
	}

	fmt.Println("GetNode: ", node)

	// GetNodesByRegex(regex string) ([]Node, error)
	nodes, errGetNodes := mg.GetNodesByRegex(".*")
	if errGetNodes != nil {
		t.Errorf("Error: %v", errGetNodes)
	}

	fmt.Println("GetNodesByRegex: ", nodes)

	// GetEdgesByRegex(regex string) ([]Edge, error)
	edges, errGetEdges := mg.GetEdgesByRegex(".*")
	if errGetEdges != nil {
		t.Errorf("Error: %v", errGetEdges)
	}

	fmt.Println("GetEdgesByRegex: ", edges)

	// GetFromNodes(name interface{}) ([]Node, error)
	// GetToNodes(name interface{}) ([]Node, error)
	// GetInEdges(name interface{}) ([]Edge, error)
	// GetOutEdges(name interface{}) ([]Edge, error)

	//+ DeleteNode(name interface{}) error
	err11 := mg.DeleteNode("AliBaba")
	if err11 != nil {
		t.Errorf("Error: %v", err11)
	}

	err11 = mg.DeleteNode("Facebook")
	if err11 != nil {
		t.Errorf("Error: %v", err11)
	}

	//+ DeleteItemByID(id interface{}) error
	err12 := mg.DeleteItemByID(newEdge.ID)
	if err12 != nil {
		t.Errorf("Error: %v", err12)
	}

}

func TestGet(t *testing.T) {
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	err1 := mg.Connect()
	if err1 != nil {
		t.Errorf("Error: %v", err1)
	}

	// add some nodes and edges
	// check if the collection exists, if yes drop it
	if mg.collectionExists("Company") {
		err2 := mg.dropCollection("Company")
		if err2 != nil {
			t.Errorf("Error: %v", err2)
		}
	}

	// add a node should be successful
	node := Node{
		Collection: "Company",
		Name:       "Google",
		Data: map[string]interface{}{
			"location":  "Mountain View",
			"employees": 1000,
		},
	}
	node1 := Node{
		Collection: "Company",
		Name:       "Facebook",
		Data: map[string]interface{}{
			"location":  "Menlo Park",
			"employees": 2000,
		},
	}
	node2 := Node{
		Collection: "Company",
		Name:       "Apple",
		Data: map[string]interface{}{
			"location":  "Cupertino",
			"employees": 5000,
		},
	}

	res, err3 := mg.AddNode(&node)

	if err3 != nil {
		t.Errorf("Error: %v", err3)
	}

	// add another node should be successful
	res1, err4 := mg.AddNode(&node1)
	if err4 != nil {
		t.Errorf("Error: %v", err4)
	}

	res2, err5 := mg.AddNode(&node2)

	if err5 != nil {
		t.Errorf("Error: %v", err5)
	}
	fmt.Println(res)
	fmt.Println(res1)
	fmt.Println(res2)
	// check if the collection exists, if yes drop it
	if mg.collectionExists("Company-Company") {
		err6 := mg.dropCollection("Company-Company")
		if err6 != nil {
			t.Errorf("Error: %v", err6)
		}
	}

	// add an edge should be successful
	edge := Edge{
		Collection: "Company-Company",
		From:       res.(primitive.ObjectID),
		To:         res1.(primitive.ObjectID),
		Data: map[string]interface{}{
			"relation": "partnership",
		},
	}

	edge1 := Edge{
		Collection: "Company-Company",
		From:       res1.(primitive.ObjectID),
		To:         res2.(primitive.ObjectID),
		Data: map[string]interface{}{
			"relation": "partnership",
		},
	}

	edge2 := Edge{
		Collection: "Company-Company",
		From:       res2.(primitive.ObjectID),
		To:         res.(primitive.ObjectID),
		Data: map[string]interface{}{
			"relation": "partnership",
		},
	}
	edge3 := Edge{
		Collection: "Company-Company",
		From:       res2.(primitive.ObjectID),
		To:         res1.(primitive.ObjectID),
		Data: map[string]interface{}{
			"relation": "partnership",
		},
	}

	res7, err7 := mg.AddEdge(&edge)
	if err7 != nil {
		t.Errorf("Error: %v", err7)
	}

	res8, err8 := mg.AddEdge(&edge1)
	if err8 != nil {
		t.Errorf("Error: %v", err8)
	}

	res9, err9 := mg.AddEdge(&edge2)
	if err9 != nil {
		t.Errorf("Error: %v", err9)
	}

	res10, err10 := mg.AddEdge(&edge3)
	if err10 != nil {
		t.Errorf("Error: %v", err10)
	}

	fmt.Println(res7)
	fmt.Println(res8)
	fmt.Println(res9)
	fmt.Println(res10)

	// get the from nodes
	fromNodes, err9 := mg.GetFromNodes("Apple")
	if err9 != nil {
		t.Errorf("Error: %v", err9)
	}

	fmt.Println(fromNodes)
	// get the to edges
	toNodes, err10 := mg.GetToNodes("Apple")
	if err10 != nil {
		t.Errorf("Error: %v", err10)
	}

	fmt.Println(toNodes)

	// get the in edges
	inEdges, err11 := mg.GetInEdges("Apple")
	if err11 != nil {
		t.Errorf("Error: %v", err11)
	}

	fmt.Println(inEdges)

	// get the out edges
	outEdges, err12 := mg.GetOutEdges("Apple")
	if err12 != nil {
		t.Errorf("Error: %v", err12)
	}

	fmt.Println(outEdges)


	// + Graph operations
	// - Traversal operations
	nodeslice, errgo:=mg.GetAllRelatedNodes("Apple")
	if errgo != nil {
		t.Errorf("Error: %v", errgo)
	}

	fmt.Println(nodeslice)

	nodeslice1, errgo1:=mg.GetAllRelatedNodesInEdgeSlice("Apple", &edge, &edge1)
	if errgo1 != nil {
		t.Errorf("Error: %v", errgo1)
	}


	fmt.Println(nodeslice1)


	nodeslice2, errgo2:=mg.GetAllRelatedNodesInRange("Apple", 2)
	if errgo2 != nil {
		t.Errorf("Error: %v", errgo2)
	}

	fmt.Println(nodeslice2)

	



}
