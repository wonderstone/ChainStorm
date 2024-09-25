package arango

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/wonderstone/chainstorm/handler"
	"gopkg.in/yaml.v3"
)

// - Init operations
// Init(yamlPath string) error
func (ag *ArangoGraph) Init(yamlPath string) error {
	// read the yaml file
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
	ag.username = data["username"].(string)
	ag.password = data["password"].(string)
	ag.server = data["server"].(string)
	ag.port = data["port"].(int)
	ag.dbname = data["dbname"].(string)
	ag.Name = data["name"].(string)

	return nil
}

// - Connection operations
// Connect() error

func (ag *ArangoGraph) Connect() error {
	// connect to the arango database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{ag.server + ":" + strconv.Itoa((ag.port))},
	})
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}

	ag.Client, err = driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(ag.username, ag.password),
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}



	// get the database
	ag.db, err = ag.Client.Database(context.TODO(), ag.dbname)

	if err != nil {
		return err
	}

	// get the graph
	// ag.graph, err = ag.db.Graph(context.TODO(), ag.Name)

	// if err != nil {
	// 	return err
	// }

	return nil
}

// Disconnect() error

func (ag *ArangoGraph) Disconnect() error {
	// ! I think, ArangoDB does not need a disconnect operation
	// ! ArangoDB exposes its API via HTTP methods (e.g. GET, POST, PUT, DELETE)
	// ! If the client does not send a Connection header in its request, 
	// ! ArangoDB will assume the client wants to keep alive the connection. 
	// ! If clients do not wish to use the keep-alive feature, 
	// ! they should explicitly indicate that by sending a Connection: Close HTTP header in the request.
	// https://docs.arangodb.com/3.10/develop/http-api/general-request-handling/
	return nil
}


// createGraph
func (ag *ArangoGraph) createGraph() error {

	options := driver.CreateGraphOptions{
		EdgeDefinitions: []driver.EdgeDefinition{
			{
				Collection: "knows",
				From:       []string{"persons"},
				To:         []string{"persons"},
			},
		},
	}

	// create the graph
	var err error
	ag.graph, err = ag.db.CreateGraph(context.TODO(), ag.Name, &options)

	if err != nil {
		return err
	}

	return nil
}

// - CRUD operations
// // + Create operations
// AddNode(n Node) (interface{}, error)
// func (ag *ArangoGraph) AddNode(ni handler.Node) (interface{}, error) {
// 	// convert the handler.Node to Node
// 	var n Node
// 	switch v := ni.(type) {
// 	case *Node:
// 		n = *v
// 	default:
// 		return nil, fmt.Errorf("invalid input")
// 	}
// 	// get nodeCol from n



// 	nodeCol, err := ag.graph.VertexCollection(context.Background(), "persons")
// 	if err != nil {
// 		return nil, err
// 	}

// 	meta, err := nodeCol.CreateDocument(context.Background(), n)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return meta, nil

// }

func (ag *ArangoGraph) AddNode(ni handler.Node) (interface{}, error) {

	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return nil, fmt.Errorf("invalid input")
	}
	// add node to the arangodb
	ctx := context.Background()
	// # Open a database
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	// # check if the collection exists
	if exists, err := db.CollectionExists(ctx, n.Collection); err != nil {
		log.Fatalf("Failed to check for collection: %v", err)
	} else {
		if !exists {
			// create a collection
			col, err := db.CreateCollection(ctx, n.Collection, nil)
			if err != nil {
				log.Fatalf("Failed to create collection: %v", err)
			}
			log.Printf("Collection %s created", col.Name())
		}
	}

	// # Open a collection
	col, err := db.Collection(ctx, n.Collection)
	if err != nil {
		log.Fatalf("Failed to open collection: %v", err)
	}

	// # create a document
	doc := n.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	meta, err := col.CreateDocument(ctx, doc)
	if err != nil {
		log.Fatalf("Failed to create document: %v", err)
	}

	return meta, nil
}

// AddEdge(e Edge) (interface{}, error)
func (ag *ArangoGraph) AddEdge(ei handler.Edge) (interface{}, error) {
	// convert the handler.Edge to Edge
	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		return nil, fmt.Errorf("invalid input")
	}

	// open the edge collection
	db, err := ag.Client.Database(context.Background(), ag.dbname)
	if err != nil {

		return nil, err
	}

	edgeCol, err := db.Collection(context.Background(), e.Collection)
	if err != nil {
		return nil, err
	}

	doc := e.Data
	doc["_id"] = e.ID
	doc["_from"] = e.From
	doc["_to"] = e.To
	doc["collection"] = e.Collection

	

	meta, err := edgeCol.CreateDocument(context.Background(), doc)
	if err != nil {
		return nil, err
	}

	return meta, nil

}


// func (ag *ArangoGraph) AddEdge(ei handler.Edge) (interface{}, error) {
// 	// convert the handler.Edge to Edge
// 	var e Edge
// 	switch v := ei.(type) {
// 	case *Edge:
// 		e = *v
// 	default:
// 		return nil, fmt.Errorf("invalid input")
// 	}

// 	edgeCol, constraints ,err := ag.graph.EdgeCollection(context.Background(), "knows")
// 	if err != nil {
// 		return nil, err
// 	}
// 	fmt.Println(constraints)

// 	meta, err := edgeCol.CreateDocument(context.Background(), e)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return meta, nil
// }
// // + Update operations
// ReplaceNode(n Node) error
// ReplaceEdge(e Edge) error
// UpdateNode(n Node) error
// UpdateEdge(e Edge) error
// MergeNode(n Node) error
// MergeEdge(e Edge) error
// // + Delete operations
// DeleteNode(name interface{}) error
// DeleteItemByID(id interface{}) error

// // + Query operations
// GetItemByID(id interface{}) (interface{}, error)
// GetNode(name interface{}) (Node, error)
// GetNodesByRegex(regex string) ([]Node, error)
// GetEdgesByRegex(regex string) ([]Edge, error)

// GetFromNodes(name interface{}) ([]Node, error)
// GetToNodes(name interface{}) ([]Node, error)
// GetInEdges(name interface{}) ([]Edge, error)
// GetOutEdges(name interface{}) ([]Edge, error)

// // + Graph operations
// // - Traversal operations
// GetAllRelatedNodes(name interface{}) ([][]Node, error)
// GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...Edge) ([][]Node, error)
// // GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)
