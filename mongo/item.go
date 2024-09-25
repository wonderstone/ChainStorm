// $ mongo solution is not efficient
// $ some effort is made but not enough

package mongo

import (
	"context"
	"fmt"
	"os"

	"github.com/wonderstone/chainstorm/handler"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v3"
)

type void struct{}

// + item in the real world should be represented by a node
type Node struct {
	ID         primitive.ObjectID     `bson:"_id,omitempty"`
	Collection string                 `bson:"collection"`
	Name       string                 `bson:"name"`
	Data       map[string]interface{} `bson:"data"`
}

func (v *Node) Export() map[string]interface{} {
	return map[string]interface{}{
		"collection": v.Collection,
		"name":       v.Name,
		"data":       v.Data,
	}
}

func isNode(doc interface{}) bool {
	_, ok := doc.(Node)
	return ok
}

// check map has all node keys
func hasNodeKeys(doc map[string]interface{}) bool {
	_, ok1 := doc["name"]
	_, ok2 := doc["data"]
	_, ok3 := doc["collection"]
	return ok1 && ok2 && ok3
}

// change the map to node
func mapToNode(doc map[string]interface{}) (Node, error) {
	// check if the map has all the keys
	if !hasNodeKeys(doc) {
		return Node{}, fmt.Errorf("invalid map")
	}
	// change the map to node
	col, ok := doc["collection"].(string)
	if !ok {
		return Node{}, fmt.Errorf("invalid collection")
	}

	name, ok := doc["name"].(string)
	if !ok {
		return Node{}, fmt.Errorf("invalid name")
	}

	// check the data type
	// if the data is a primitive.M, convert it to map[string]interface{}
	// if the data is a map[string]interface{}, convert it to map[string]interface{}
	var data map[string]interface{}
	switch v := doc["data"].(type) {
	case primitive.M:
		data = map[string]interface{}(v)
	case map[string]interface{}:
		data = v
	default:
		return Node{}, fmt.Errorf("invalid data")
	}

	node := Node{
		Collection: col,
		Name:       name,
		Data:       data,
	}

	if _, ok := doc["_id"]; ok {
		node.ID = doc["_id"].(primitive.ObjectID)
	}
	return node, nil
}

// + relationship between two nodes represented by an edge
type Edge struct {
	ID           primitive.ObjectID     `bson:"_id,omitempty"`
	From         primitive.ObjectID     `bson:"from"`
	To           primitive.ObjectID     `bson:"to"`
	Collection   string                 `bson:"collection"`
	Relationship string                 `bson:"relationship"`
	Data         map[string]interface{} `bson:"data,omitempty"`
}

func (e *Edge) Export() map[string]interface{} {
	return map[string]interface{}{
		"from":         e.From,
		"to":           e.To,
		"collection":   e.Collection,
		"relationship": e.Relationship,
		"data":         e.Data,
	}
}

func isEdge(doc interface{}) bool {
	_, ok := doc.(Edge)
	return ok
}

// check map has all edge keys
func hasEdgeKeys(doc map[string]interface{}) bool {
	_, ok1 := doc["from"]
	_, ok2 := doc["to"]
	_, ok3 := doc["collection"]
	_, ok4 := doc["relationship"]
	return ok1 && ok2 && ok3 && ok4
}

// change the map to edge
func mapToEdge(doc map[string]interface{}) (Edge, error) {
	// check if the map has all the keys
	if !hasEdgeKeys(doc) {
		return Edge{}, fmt.Errorf("invalid map")
	}
	// change the map to edge
	from, ok := doc["from"].(primitive.ObjectID)
	if !ok {
		return Edge{}, fmt.Errorf("invalid from")
	}

	to, ok := doc["to"].(primitive.ObjectID)
	if !ok {
		return Edge{}, fmt.Errorf("invalid to")
	}

	col, ok := doc["collection"].(string)
	if !ok {
		return Edge{}, fmt.Errorf("invalid collection")
	}

	rel, ok := doc["relationship"].(string)
	if !ok {
		return Edge{}, fmt.Errorf("invalid relationship")
	}

	edge := Edge{
		From:         from,
		To:           to,
		Collection:   col,
		Relationship: rel,
	}

	if _, ok := doc["_id"]; ok {
		edge.ID = doc["_id"].(primitive.ObjectID)
	}

	if _, ok := doc["data"]; ok {
		var data map[string]interface{}
		switch v := doc["data"].(type) {
		case primitive.M:
			data = map[string]interface{}(v)
		case map[string]interface{}:
			data = v
		default:
			return Edge{}, fmt.Errorf("invalid data")
		}
		edge.Data = data
	}

	return edge, nil
}

// + the whole graph is a collection of nodes and edges
// + explain the items and relationships between them
type MongoGraph struct {
	// = section for mongoDB connection
	username string
	password string
	server   string
	port     int
	database string

	// = section for better performance
	// * collSet to store the created collection names
	// * for both nodes and edges
	collSet map[string]void

	// * nodeNameCollMap with type map[name]collection
	// * to prevent iteration over all collections
	nodeNameCollMap map[string]string

	// * itemSet with type map[primitive.ObjectID]void to store the item IDs for further check
	// # however primitive.ObjectID contains a byte slice ([12]byte), which makes it non-comparable
	// # so use string and primitive.ObjectID.Hex() to store the ID
	itemSet map[string]void

	client *mongo.Client
}

// - implement Init operations
func (mg *MongoGraph) Init(yamlPath string) error {
	var err error
	// = recover from panic
	defer recoverFromPanic(&err)

	yamlData, err := os.ReadFile(yamlPath)
	if err != nil {
		return err
	}

	// = unmarshal the yaml data into a map
	var data map[string]interface{}
	err = yaml.Unmarshal(yamlData, &data)
	if err != nil {
		return err
	}

	// = section for mongoDB connection
	mg.username = data["username"].(string)
	mg.password = data["password"].(string)
	mg.server = data["server"].(string)
	mg.port = data["port"].(int)
	mg.database = data["database"].(string)

	// = section for better performance
	mg.collSet = make(map[string]void)
	mg.nodeNameCollMap = make(map[string]string)
	mg.itemSet = make(map[string]void)

	return err
}

// - implement Connection operations
// + Connect also update the collset, nodeNameCollMap and itemSet
func (mg *MongoGraph) Connect() error {
	// @ build the uri with the username , password , server and port
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d", mg.username, mg.password, mg.server, mg.port)
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}
	mg.client = client

	// // @ get all the collections in the database
	// collections, err := client.Database(mg.database).ListCollectionNames(context.Background(), bson.D{})
	// if err != nil {
	// 	return err
	// }

	// // @ update the collSet
	// mg.collSet = make(map[string]void)
	// for _, col := range collections {
	// 	mg.collSet[col] = void{}
	// }

	// @ update the nodeNameCollMap and itemSet
	err = mg.updateNameCollMap_IDSet()
	if err != nil {
		return err
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

// iterate all nodes in database to update the nameCollectionMap
func (mg *MongoGraph) updateNameCollMap_IDSet() error {
	// get the database
	db := mg.client.Database(mg.database)
	// get all the collections in the database
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return err
	}
	// make mg.nodeNameCollMap , mg.itemSet and mg.collSet  empty
	mg.nodeNameCollMap = make(map[string]string)
	mg.itemSet = make(map[string]void)
	mg.collSet = make(map[string]void)

	// iterate all the collections
	for _, col := range collections {

		// @ update the collSet
		mg.collSet[col] = void{}

		// get the collection
		// colName := col
		col := db.Collection(col)
		// get all the nodes in the collection
		cursor, err := col.Find(context.Background(), bson.D{})
		if err != nil {
			return err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection
		// only when the element is a Node, update the nameCollectionMap
		for cursor.Next(context.Background()) {
			// ! cannot use cursor.Decode(&node) or cursor.Decode(&edge) directly
			// ! because the doc can both decode to Node and Edge with blank fields
			// ! so decode the doc to primitive.M first

			// & decode the doc to primitive.M first
			// & node has name key, edge has from and to key
			// & if the doc has node signiture key, then decode it to Node and update nodeNameCollMap and itemSet
			// & if the doc has edge signiture key, then decode it to Edge and update itemSet

			var doc primitive.M
			err_decode := cursor.Decode(&doc)
			if err_decode != nil {
				return err_decode
			}
			// ! ugly code below
			// if doc has a key from, it is an edge
			_, isEdge := doc["from"]
			_, isNode := doc["name"]
			// ! ugly code above
			if isEdge {
				mg.itemSet[doc["_id"].(primitive.ObjectID).Hex()] = void{}
			} else if isNode {
				mg.nodeNameCollMap[doc["name"].(string)] = col.Name()
				mg.itemSet[doc["_id"].(primitive.ObjectID).Hex()] = void{}
			} else {
				return fmt.Errorf("invalid document")
			}
		}
	}
	return nil
}

// - implement CRUD operations
// + Create operations
// func to check if the collection exists
func (mg *MongoGraph) collectionExists(collection string) bool {
	_, ok := mg.collSet[collection]
	return ok
}

// func to create new collection for node
// ~ with the name as unique index
// ~ mongoDB normally creates collection when inserting data if the collection does not exist
// ~ but dealing with the specific configuration, create the collection explicitly
func (mg *MongoGraph) createCollection(collection string) error {
	// get the database
	db := mg.client.Database(mg.database)
	// index model
	indexModel := mongo.IndexModel{
		Keys:    bson.M{"name": 1},
		Options: options.Index().SetUnique(true),
	}
	// create the collection
	err := db.CreateCollection(context.Background(), collection)
	if err != nil {
		return err
	}

	// create the index
	_, err = db.Collection(collection).Indexes().CreateOne(context.Background(), indexModel)
	if err != nil {
		return err
	}
	// add the collection to the collectionSet
	mg.collSet[collection] = void{}
	return nil
}

// func to drop a collection
func (mg *MongoGraph) dropCollection(collection string) error {
	// get the database
	db := mg.client.Database(mg.database)
	// drop the collection
	err := db.Collection(collection).Drop(context.Background())
	if err != nil {
		return err
	}
	// delete the collection from the collectionSet
	delete(mg.collSet, collection)
	//
	return nil
}

// implement the AddNode method，
// return the inserted ID and error
func (mg *MongoGraph) AddNode(ni handler.Node) (interface{}, error) {
	// check ni type
	// if ni is a pointer, use ni.(*Node)
	// if ni is a value, use ni.(Node)
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return nil, fmt.Errorf("invalid input")
	}

	var err error
	defer recoverFromPanic(&err)
	// check if the collection exists, if not create the collection
	if !mg.collectionExists(n.Collection) {
		// create the collection
		err = mg.createCollection(n.Collection)
		if err != nil {
			return nil, err
		}
	}

	// right now the collection exists and has name as unique index
	// insert the node
	db := mg.client.Database(mg.database)
	verticesCol := db.Collection(n.Collection)

	// insert the node
	res, err := verticesCol.InsertOne(context.TODO(), n)
	if err != nil {
		return nil, err
	}
	// update the nameCollectionMap
	mg.nodeNameCollMap[n.Name] = n.Collection
	// update the itemSet
	mg.itemSet[res.InsertedID.(primitive.ObjectID).Hex()] = void{}
	return res.InsertedID, nil
}

// implement the AddEdge method
// return the inserted ID and error
func (mg *MongoGraph) AddEdge(ei handler.Edge) (interface{}, error) {
	// check ei type
	// if ei is a pointer, use ei.(*Edge)
	// if ei is a value, use ei.(Edge)
	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		return nil, fmt.Errorf("invalid input")
	}

	var err error
	defer recoverFromPanic(&err)

	// get the database and collection
	db := mg.client.Database(mg.database)
	edgesCol := db.Collection(e.Collection)

	// check if the from and to nodes exist by using itemSet
	if _, ok := mg.itemSet[e.From.Hex()]; !ok {
		return nil, fmt.Errorf("from node not found")
	}
	if _, ok := mg.itemSet[e.To.Hex()]; !ok {
		return nil, fmt.Errorf("to node not found")
	}
	// ! refactored the code
	// ! check if the from and to nodes exist by using GetItemByID
	// ! GetItemByID is not efficient because it iterates all the collections
	// _, err = mg.GetItemByID(e.From)
	// if err != nil {
	// 	return nil, fmt.Errorf("From node not found")
	// }

	// _, err = mg.GetItemByID(e.To)
	// if err != nil {
	// 	return nil, fmt.Errorf("To node not found")
	// }

	// insert the edge
	res, err := edgesCol.InsertOne(context.TODO(), e)

	if err != nil {
		return nil, err
	}

	return res.InsertedID, nil
}

// + Query operations
// implement the GetNode method
// return the node and error
func (mg *MongoGraph) GetNode(name interface{}) (handler.Node, error) {
	var err error
	defer recoverFromPanic(&err)

	// get the database and collection
	db := mg.client.Database(mg.database)

	if _, ok := name.(string); ok {
		// get the collection name
		if _, ok := mg.nodeNameCollMap[name.(string)]; !ok {
			return &Node{}, fmt.Errorf("Node not found")
		}
		colName := mg.nodeNameCollMap[name.(string)]
		nodeName := name.(string)
		verticesCol := db.Collection(colName)

		// find the node
		var node Node
		err = verticesCol.FindOne(context.TODO(), bson.M{"name": nodeName}).Decode(&node)
		if err != nil {
			return &Node{}, err
		}
		return &node, nil
	} else {
		return &Node{}, fmt.Errorf("invalid input, should be string")
	}

}

// func GetItemByID to get the item by ID
// iter evey time return the item in primitive.M type and error
// not the best solution
func (mg *MongoGraph) GetItemByID(id interface{}) (interface{}, error) {
	var err error
	defer recoverFromPanic(&err)
	// fmt.Println("ID", id)
	// get the database
	db := mg.client.Database(mg.database)

	// get all the collections in the database
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// iterate all the collections
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the item by ID
		var item primitive.M
		err_decode := col.FindOne(context.Background(), bson.M{"_id": id}).Decode(&item)

		if err_decode == nil {
			return item, nil
		}

	}
	return nil, fmt.Errorf("item not found")
}

// GetNodesByRegex(regex string) ([]Node, error)
// regex is the regular expression for the name
func (mg *MongoGraph) GetNodesByRegex(regex string) ([]handler.Node, error) {
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the nodes
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the nodes
	var nodes []handler.Node
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the nodes
		cursor, err := col.Find(context.Background(), bson.M{"name": bson.M{"$regex": regex}})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// append the node to the nodes
			tmpNode, errtmp := mapToNode(doc)
			if errtmp != nil {
				return nil, errtmp
			}
			nodes = append(nodes, &tmpNode)
		}

	}
	return nodes, nil
}

// GetEdgesByRegex(regex string) ([]Edge, error)
// regex is the regular expression for the relationship
func (mg *MongoGraph) GetEdgesByRegex(regex string) ([]handler.Edge, error) {
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the edges
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the edges
	var edges []handler.Edge
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the edges
		cursor, err := col.Find(context.Background(), bson.M{"relationship": bson.M{"$regex": regex}})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// append the edge to the edges
			tmpEdge, errtmp := mapToEdge(doc)
			if errtmp != nil {
				return nil, errtmp
			}
			edges = append(edges, &tmpEdge)
		}

	}
	return edges, nil
}

// implement GetFromNodes(name interface{}) ([]Node, error)

func (mg *MongoGraph) GetFromNodes(name interface{}) ([]handler.Node, error) {
	// get node by name
	nodetmp, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}
	node := nodetmp.(*Node)
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the from nodes
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the from nodes
	var fromNodes []handler.Node
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the from nodes
		cursor, err := col.Find(context.Background(), bson.M{"to": node.ID})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// get the from node id
			fromNodeID := doc["from"].(primitive.ObjectID)
			// get the from node by id
			fromNode, err := mg.GetItemByID(fromNodeID)
			if err != nil {
				return nil, err
			}
			// append the from node to the fromNodes

			tmpNode, errtmp := mapToNode(fromNode.(primitive.M))
			if errtmp != nil {
				return nil, errtmp
			}

			fromNodes = append(fromNodes, &tmpNode)
		}

	}
	return fromNodes, nil

}

// GetFromNodesInEdges(name interface{}, edges ... Edge) ([]Node, error)
func (mg *MongoGraph) GetFromNodesInEdges(name interface{}, edges ...handler.Edge) ([]handler.Node, error) {

	var edgeSet = make(map[string]void)

	// iter the edges to get the edgeSet
	for _, edgetmp := range edges {
		edge := edgetmp.(*Edge)
		edgeSet[edge.ID.Hex()] = void{}
	}

	// get node by name
	nodetmp, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}
	node := nodetmp.(*Node)
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the from nodes
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the from nodes
	var fromNodes []handler.Node
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the from nodes
		cursor, err := col.Find(context.Background(), bson.M{"to": node.ID})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iter all the matching edges and only edge in the edgeSet will be considered

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// get the edge id
			edgeID := doc["_id"].(primitive.ObjectID)
			// check if the edge is in the edgeSet
			if _, ok := edgeSet[edgeID.Hex()]; !ok {
				continue
			}
			// get the from node id
			fromNodeID := doc["from"].(primitive.ObjectID)
			// get the from node by id
			fromNode, err := mg.GetItemByID(fromNodeID)
			if err != nil {
				return nil, err
			}

			// append the from node to the fromNodes
			tmpNode, errtmp := mapToNode(fromNode.(primitive.M))
			if errtmp != nil {
				return nil, errtmp
			}

			fromNodes = append(fromNodes, &tmpNode)
		}

	}
	return fromNodes, nil
}

// implement GetToNodes(name interface{}) ([]Node, error)
func (mg *MongoGraph) GetToNodes(name interface{}) ([]handler.Node, error) {
	// get node by name
	nodetmp, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}

	node := nodetmp.(*Node)
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the to nodes
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the to nodes
	var toNodes []handler.Node
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the to nodes
		cursor, err := col.Find(context.Background(), bson.M{"from": node.ID})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// get the to node id
			toNodeID := doc["to"].(primitive.ObjectID)
			// get the to node by id
			toNode, err := mg.GetItemByID(toNodeID)
			if err != nil {
				return nil, err
			}
			// append the to node to the toNodes

			tmpNode, errtmp := mapToNode(toNode.(primitive.M))
			if errtmp != nil {
				return nil, errtmp
			}

			toNodes = append(toNodes, &tmpNode)
		}

	}
	return toNodes, nil
}

// GetToNodesInEdges(name interface{}, edges ... Edge) ([]Node, error)
func (mg *MongoGraph) GetToNodesInEdges(name interface{}, edges ...handler.Edge) ([]handler.Node, error) {
	var edgeSet = make(map[string]void)

	// iter the edges to get the edgeSet
	for _, edgetmp := range edges {
		edge := edgetmp.(*Edge)
		edgeSet[edge.ID.Hex()] = void{}
	}

	// get node by name
	nodetmp, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}
	node := nodetmp.(*Node)
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the to nodes
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the to nodes
	var toNodes []handler.Node
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the to nodes
		cursor, err := col.Find(context.Background(), bson.M{"from": node.ID})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iter all the matching edges and only edge in the edgeSet will be considered

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// get the edge id
			edgeID := doc["_id"].(primitive.ObjectID)
			// check if the edge is in the edgeSet
			if _, ok := edgeSet[edgeID.Hex()]; !ok {
				continue
			}
			// get the to node id
			toNodeID := doc["to"].(primitive.ObjectID)
			// get the to node by id
			toNode, err := mg.GetItemByID(toNodeID)
			if err != nil {
				return nil, err
			}

			// append the to node to the toNodes
			tmpNode, errtmp := mapToNode(toNode.(primitive.M))
			if errtmp != nil {
				return nil, errtmp
			}

			toNodes = append(toNodes, &tmpNode)
		}

	}
	return toNodes, nil
}

// implement GetInEdges(name interface{}) ([]Edge, error)
func (mg *MongoGraph) GetInEdges(name interface{}) ([]handler.Edge, error) {
	// get node by name
	nodetmp, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}
	node := nodetmp.(*Node)
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the in edges
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the in edges
	var inEdges []handler.Edge
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the in edges
		cursor, err := col.Find(context.Background(), bson.M{"to": node.ID})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// get the in edge id
			inEdgeID := doc["_id"].(primitive.ObjectID)
			// get the in edge by id
			inEdge, err := mg.GetItemByID(inEdgeID)
			if err != nil {
				return nil, err
			}
			// append the in edge to the inEdges
			var tmp map[string]interface{}
			switch v := inEdge.(type) {
			case primitive.M:
				tmp = map[string]interface{}(v)
			case map[string]interface{}:
				tmp = v
			default:
				return nil, fmt.Errorf("invalid type")
			}

			tmpEdge, errtmp := mapToEdge(tmp)
			if errtmp != nil {
				return nil, errtmp
			}

			inEdges = append(inEdges, &tmpEdge)
		}

	}
	return inEdges, nil
}

// GetOutEdges(name interface{}) ([]Edge, error)
func (mg *MongoGraph) GetOutEdges(name interface{}) ([]handler.Edge, error) {
	// get node by name
	nodetmp, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}
	node := nodetmp.(*Node)
	// get the database
	db := mg.client.Database(mg.database)
	// iter the items in the database to get the out edges
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}

	// get the out edges
	var outEdges []handler.Edge
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// get the out edges
		cursor, err := col.Find(context.Background(), bson.M{"from": node.ID})
		if err != nil {
			return nil, err
		}
		defer cursor.Close(context.Background())

		// iterate the elements in the collection

		for cursor.Next(context.Background()) {
			var doc primitive.M
			err = cursor.Decode(&doc)
			if err != nil {
				return nil, err
			}
			// get the out edge id
			outEdgeID := doc["_id"].(primitive.ObjectID)
			// get the out edge by id
			outEdge, err := mg.GetItemByID(outEdgeID)
			if err != nil {
				return nil, err
			}
			// append the out edge to the outEdges

			tmpEdge, errtmp := mapToEdge(outEdge.(primitive.M))
			if errtmp != nil {
				return nil, errtmp
			}

			outEdges = append(outEdges, &tmpEdge)
		}

	}
	return outEdges, nil
}

// + Update operations
// ReplaceNode(n Node) (err error)
// Replace: only ID keeps the same, other fields will be replaced
func (mg *MongoGraph) ReplaceNode(ni handler.Node) error {
	var err error
	defer recoverFromPanic(&err)
	// check ni type
	// if ni is a pointer, use ni.(*Node)
	// if ni is a value, use ni.(Node)
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}
	// get the database and collection
	db := mg.client.Database(mg.database)
	verticesCol := db.Collection(n.Collection)

	// replace the node with the same id
	_, err = verticesCol.ReplaceOne(context.TODO(), bson.M{"_id": n.ID}, n)
	if err != nil {
		return err
	}
	return nil
}

// ReplaceEdge(e Edge) (id interface{}, err error)
// Replace: only ID keeps the same, other fields will be replaced
func (mg *MongoGraph) ReplaceEdge(ei handler.Edge) error {
	var err error
	defer recoverFromPanic(&err)

	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// get the database and collection
	db := mg.client.Database(mg.database)
	edgesCol := db.Collection(e.Collection)

	// replace the edge with the same id
	_, err = edgesCol.ReplaceOne(context.TODO(), bson.M{"_id": e.ID}, e)
	if err != nil {
		return err
	}
	return nil
}

// UpdateNode(n Node) (id Node,err error)
// Update: only update the fields that are not blank
func (mg *MongoGraph) UpdateNode(ntmp handler.Node) error {
	var err error
	defer recoverFromPanic(&err)

	var n Node
	switch v := ntmp.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}
	// get the database and collection
	db := mg.client.Database(mg.database)
	verticesCol := db.Collection(n.Collection)

	// update the node with the same id
	_, err = verticesCol.UpdateOne(context.TODO(), bson.M{"_id": n.ID}, bson.M{"$set": n})
	if err != nil {
		return err
	}
	return nil
}

// UpdateEdge(e Edge) (id Edge,err error)
// Update: only update the fields that are not blank
func (mg *MongoGraph) UpdateEdge(etmp handler.Edge) error {
	var err error
	defer recoverFromPanic(&err)
	var e Edge
	switch v := etmp.(type) {
	case *Edge:
		e = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// get the database and collection
	db := mg.client.Database(mg.database)
	edgesCol := db.Collection(e.Collection)

	// update the edge with the same id
	_, err = edgesCol.UpdateOne(context.TODO(), bson.M{"_id": e.ID}, bson.M{"$set": e})
	if err != nil {
		return err
	}
	return nil
}

// MergeNode(n Node) (id Node,err error)
// Merge: data field will be merged,
//
//	other fields except ID will be replaced
func (mg *MongoGraph) MergeNode(ntmp handler.Node) error {
	var err error
	defer recoverFromPanic(&err)
	var n Node
	switch v := ntmp.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}
	// get the database and collection
	db := mg.client.Database(mg.database)
	verticesCol := db.Collection(n.Collection)

	// get the node with the same id
	var node Node
	err = verticesCol.FindOne(context.TODO(), bson.M{"_id": n.ID}).Decode(&node)
	if err != nil {
		return err
	}

	// merge the data field, keep the n untouched
	for k, v := range n.Data {
		node.Data[k] = v
	}
	// // if the name is not blank, replace the name
	// if n.Name != "" {
	// 	node.Name = n.Name
	// }
	// // if the collection is not blank, replace the collection
	// if n.Collection != "" {
	// 	node.Collection = n.Collection
	// }

	// replace the node with the same id
	_, err = verticesCol.UpdateOne(context.TODO(), bson.M{"_id": n.ID}, bson.M{"$set": n})
	if err != nil {
		return err
	}

	return nil
}

// MergeEdge(e Edge) (id Edge,err error)
// Merge: data field will be merged,
//
//	other fields except ID will be replaced
func (mg *MongoGraph) MergeEdge(etmp handler.Edge) error {
	var err error
	defer recoverFromPanic(&err)
	var e Edge
	switch v := etmp.(type) {
	case *Edge:
		e = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// get the database and collection
	db := mg.client.Database(mg.database)
	edgesCol := db.Collection(e.Collection)

	// get the edge with the same id
	var edge Edge
	err = edgesCol.FindOne(context.TODO(), bson.M{"_id": e.ID}).Decode(&edge)
	if err != nil {
		return err
	}

	// merge the data field, keep the e untouched
	for k, v := range e.Data {
		edge.Data[k] = v
	}
	// if the from is not blank, replace the from
	if e.From != primitive.NilObjectID {
		edge.From = e.From
	}
	// if the to is not blank, replace the to
	if e.To != primitive.NilObjectID {
		edge.To = e.To
	}
	// // if the collection is not blank, replace the collection
	// if e.Collection != "" {
	// 	edge.Collection = e.Collection
	// }
	// if the relationship is not blank, replace the relationship
	if e.Relationship != "" {
		edge.Relationship = e.Relationship
	}

	// replace the edge with the same id
	_, err = edgesCol.UpdateOne(context.TODO(), bson.M{"_id": e.ID}, bson.M{"$set": edge})
	if err != nil {
		return err
	}

	return nil
}

// + Delete operations
// DeleteNode(name interface{}) error
func (mg *MongoGraph) DeleteNode(name interface{}) error {
	var err error
	defer recoverFromPanic(&err)

	// get the database and collection
	db := mg.client.Database(mg.database)

	// get the collection name
	if _, ok := mg.nodeNameCollMap[name.(string)]; !ok {
		return fmt.Errorf("Node not found")
	}
	colName := mg.nodeNameCollMap[name.(string)]
	verticesCol := db.Collection(colName)

	// delete the node with the same name
	_, err = verticesCol.DeleteOne(context.TODO(), bson.M{"name": name})
	if err != nil {
		return err
	}
	// check if the collection is empty, if so, drop the collection
	cursor, err := verticesCol.Find(context.TODO(), bson.D{})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	if !cursor.Next(context.Background()) {
		// drop the collection
		err = mg.dropCollection(colName)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteItemByID(id interface{}) error
func (mg *MongoGraph) DeleteItemByID(id interface{}) error {
	var err error
	defer recoverFromPanic(&err)

	// get the database
	db := mg.client.Database(mg.database)

	// get all the collections in the database
	collections, err := db.ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return err
	}

	// iterate all the collections
	for _, col := range collections {
		// get the collection
		col := db.Collection(col)
		// delete the item by ID
		_, err = col.DeleteOne(context.Background(), bson.M{"_id": id})
		if err == nil {
			// check if the collection is empty, if so, drop the collection
			cursor, err := col.Find(context.TODO(), bson.D{})
			if err != nil {
				return err
			}

			defer cursor.Close(context.Background())

			if !cursor.Next(context.Background()) {
				// drop the collection
				err = mg.dropCollection(col.Name())
				if err != nil {
					return err
				}
			}

			return nil

		}

	}
	return fmt.Errorf("item not found")
}

// + Graph operations
// - Traversal operations
// GetAllRelatedNodes(name interface{}) ([][]Node, error)
// like BFS, get all the related nodes,
// the first dimension is the level, the second dimension is the nodes in the level
// need to avoid the circle
func (mg *MongoGraph) GetAllRelatedNodes(name interface{}) ([][]handler.Node, error) {
    var err error
    defer recoverFromPanic(&err)

    // Get the starting node by name
    staNode, err := mg.GetNode(name)
    if err != nil {
        return nil, err
    }

	// convert the node to Node
	startNode := staNode.(*Node)
    // Initialize the BFS queue and visited set
    queue := []handler.Node{startNode}
    visited := make(map[string]bool)
    visited[startNode.ID.Hex()] = true

    // Initialize the result to store nodes level-wise
    var result [][]handler.Node
    currentLevel := []handler.Node{startNode}

    for len(queue) > 0 {
        var nextLevel []handler.Node

        // Process all nodes in the current level
        for _, node := range currentLevel {
			// node type conversion
			node := node.(*Node)
            // Get all related nodes (both incoming and outgoing edges)
            fromNodes, err := mg.GetFromNodes(node.Name)
            if err != nil {
                return nil, err
            }
            toNodes, err := mg.GetToNodes(node.Name)
            if err != nil {
                return nil, err
            }

            // Combine fromNodes and toNodes
            relatedNodes := append(fromNodes, toNodes...)

            // Add unvisited related nodes to the next level
            for _, relatedNode := range relatedNodes {
				// node type conversion
				relatedNode := relatedNode.(*Node)
                if !visited[relatedNode.ID.Hex()] {
                    visited[relatedNode.ID.Hex()] = true
                    nextLevel = append(nextLevel, relatedNode)
                    queue = append(queue, relatedNode)
                }
            }
        }

        // Add the current level to the result
        result = append(result, currentLevel)
		// ! check if the next level is empty
		if len(nextLevel) == 0 {
			break
		}

        // Move to the next level
        currentLevel = nextLevel
        queue = queue[len(currentLevel):]
    }

    return result, nil
}

// GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)
// get all the related nodes in the range of max levels
// similar to GetAllRelatedNodes, but with a max level
// need to avoid the circle

func (mg *MongoGraph) GetAllRelatedNodesInRange(name interface{}, max int) ([][]handler.Node, error) {
	var err error
	defer recoverFromPanic(&err)

	// Get the starting node by name
	staNode, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}

	// convert the node to Node
	startNode := staNode.(*Node)
	// Initialize the BFS queue and visited set
	queue := []handler.Node{startNode}
	visited := make(map[string]bool)
	visited[startNode.ID.Hex()] = true

	// Initialize the result to store nodes level-wise
	var result [][]handler.Node
	currentLevel := []handler.Node{startNode}

	for i := 0; i < max; i++ {
		var nextLevel []handler.Node

		// Process all nodes in the current level
		for _, node := range currentLevel {
			// node type conversion
			node := node.(*Node)
			// Get all related nodes (both incoming and outgoing edges)
			fromNodes, err := mg.GetFromNodes(node.Name)
			if err != nil {
				return nil, err
			}
			toNodes, err := mg.GetToNodes(node.Name)
			if err != nil {
				return nil, err
			}

			// Combine fromNodes and toNodes
			relatedNodes := append(fromNodes, toNodes...)

			// Add unvisited related nodes to the next level
			for _, relatedNode := range relatedNodes {
				// node type conversion
				relatedNode := relatedNode.(*Node)
				if !visited[relatedNode.ID.Hex()] {
					visited[relatedNode.ID.Hex()] = true
					nextLevel = append(nextLevel, relatedNode)
					queue = append(queue, relatedNode)
				}
			}
		}

		// Add the current level to the result
		result = append(result, currentLevel)
		// ! check if the next level is empty
		if len(nextLevel) == 0 {
			break
		}

		// Move to the next level
		currentLevel = nextLevel
		queue = queue[len(currentLevel):]
	}

	return result, nil
}



// GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...Edge) ([][]Node, error)
// similar to GetAllRelatedNodes, but only consider the edges in the EdgeSlice
func (mg *MongoGraph) GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...handler.Edge) ([][]handler.Node, error) {
	var err error
	defer recoverFromPanic(&err)

	// Get the starting node by name
	staNode, err := mg.GetNode(name)
	if err != nil {
		return nil, err
	}

	// convert the node to Node
	startNode := staNode.(*Node)
	// Initialize the BFS queue and visited set
	queue := []handler.Node{startNode}
	visited := make(map[string]bool)
	visited[startNode.ID.Hex()] = true

	// Initialize the result to store nodes level-wise
	var result [][]handler.Node
	currentLevel := []handler.Node{startNode}

	for len(queue) > 0 {
		var nextLevel []handler.Node

		// Process all nodes in the current level
		for _, node := range currentLevel {
			// node type conversion
			node := node.(*Node)
			// Get all related nodes (both incoming and outgoing edges)
			fromNodes, err := mg.GetFromNodesInEdges(node.Name, EdgeSlice...)
			if err != nil {
				return nil, err
			}
			toNodes, err := mg.GetToNodesInEdges(node.Name, EdgeSlice...)
			if err != nil {
				return nil, err
			}

			// Combine fromNodes and toNodes
			relatedNodes := append(fromNodes, toNodes...)

			// Add unvisited related nodes to the next level
			for _, relatedNode := range relatedNodes {
				// node type conversion
				relatedNode := relatedNode.(*Node)
				if !visited[relatedNode.ID.Hex()] {
					visited[relatedNode.ID.Hex()] = true
					nextLevel = append(nextLevel, relatedNode)
					queue = append(queue, relatedNode)
				}
			}
		}

		// Add the current level to the result
		result = append(result, currentLevel)
		// ! check if the next level is empty
		if len(nextLevel) == 0 {
			break
		}

		// Move to the next level
		currentLevel = nextLevel
		queue = queue[len(currentLevel):]
	}

	return result, nil
}












// func (mg *MongoGraph) GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...handler.Edge) ([][]handler.Node, error) {
// 	// get the node by name
// 	node, err := mg.GetNode(name)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// get all the related nodes
// 	var relatedNodes [][]handler.Node
// 	// the related nodes in the current level
// 	var currentLevelNodes []handler.Node
// 	// the related nodes in the next level
// 	var nextLevelNodes []handler.Node
// 	// the related nodes in the current level
// 	currentLevelNodes = append(currentLevelNodes, node)
// 	// the related nodes in the first level
// 	relatedNodes = append(relatedNodes, currentLevelNodes)

// 	// get all the related nodes
// 	for {
// 		// get the related nodes in the current level
// 		for _, ntmp := range currentLevelNodes {
// 			n := ntmp.(Node)
// 			// get the from nodes
// 			fromNodes, err := mg.GetFromNodesInEdges(n.Name, EdgeSlice...)
// 			if err != nil {
// 				return nil, err
// 			}
// 			// get the to nodes
// 			toNodes, err := mg.GetToNodesInEdges(n.Name, EdgeSlice...)
// 			if err != nil {
// 				return nil, err
// 			}
// 			// append the from nodes to the next level nodes
// 			nextLevelNodes = append(nextLevelNodes, fromNodes...)
// 			// append the to nodes to the next level nodes
// 			nextLevelNodes = append(nextLevelNodes, toNodes...)
// 		}
// 		// if there is no next level nodes, break
// 		if len(nextLevelNodes) == 0 {
// 			break
// 		}
// 		// append the next level nodes to the related nodes
// 		relatedNodes = append(relatedNodes, nextLevelNodes)
// 		// update the current level nodes
// 		currentLevelNodes = nextLevelNodes
// 		// clear the next level nodes
// 		nextLevelNodes = nil
// 	}
// 	return relatedNodes, nil

// }

