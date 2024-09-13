package arango

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"os"
// 	"strconv"

// 	"github.com/arangodb/go-driver"
// 	"github.com/arangodb/go-driver/http"
// 	"gopkg.in/yaml.v3"
// )

// // "context"
// // "fmt"
// // "log"
// // "time"

// // driver "github.com/arangodb/go-driver"
// // "github.com/arangodb/go-driver/http"

// type ArangoGraph struct {
// 	username string
// 	password string
// 	server   string
// 	port     int
// 	dbname   string

// 	Client   driver.Client
// 	Name     string
// }

// func NewArangoGraph() (*ArangoGraph, error) {
// 	db := &ArangoGraph{}
// 	return db, nil
// }

// func (ag *ArangoGraph) Init(yamlPath string) error {
// 	yamlData, err := os.ReadFile(yamlPath)

// 	if err != nil {
// 		return err
// 	}

// 	// unmarshal the yaml data into a map
// 	var data map[string]interface{}
// 	err = yaml.Unmarshal(yamlData, &data)

// 	if err != nil {
// 		return err
// 	}

// 	// get the yaml data
// 	ag.username = data["username"].(string)
// 	ag.password = data["password"].(string)
// 	ag.server = data["server"].(string)
// 	ag.port = data["port"].(int)
// 	ag.dbname = data["dbname"].(string)
// 	ag.Name = data["name"].(string)

// 	return nil

// }

// func (ag *ArangoGraph) Connect() error {
// 	// # Connect to ArangoDB
// 	conn, err := http.NewConnection(http.ConnectionConfig{
// 		Endpoints: []string{ag.server + ":" + strconv.Itoa((ag.port))},
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create connection: %v", err)
// 	}

// 	ag.Client, err = driver.NewClient(driver.ClientConfig{
// 		Connection:     conn,
// 		Authentication: driver.BasicAuthentication(ag.username, ag.password),
// 	})
// 	if err != nil {
// 		log.Fatalf("Failed to create client: %v", err)
// 	}

// 	return nil

// }

// // Disconnect from the ArangoDB
// // Do nothing for now
// func (ag *ArangoGraph) Disconnect() error {
// 	return nil
// }




// // type Node struct {
// // 	ID         string                 `json:"ID"`         // 节点的唯一标识符
// // 	Collection string                 `json:"Collection"` // 节点所属的集合
// // 	Name       string                 `json:"Name"`       // 节点的名称
// // 	Data       map[string]interface{} `json:"Data"`       // 节点存储的数据
// // }

// // # AddNode
// // & AddFunc Section
// func (ag *ArangoGraph) AddNode(n *base.Node) error {
// 	// add node to the arangodb
// 	ctx := context.Background()
// 	// # Open a database
// 	db, err := ag.Client.Database(ctx, ag.dbname)
// 	if err != nil {
// 		log.Fatalf("Failed to open database: %v", err)
// 	}
// 	// # check if the collection exists
// 	if exists, err := db.CollectionExists(ctx, n.Collection); err != nil {
// 		log.Fatalf("Failed to check for collection: %v", err)
// 	} else {
// 		if !exists {
// 			// create a collection
// 			col, err := db.CreateCollection(ctx, n.Collection, nil)
// 			if err != nil {
// 				log.Fatalf("Failed to create collection: %v", err)
// 			}
// 			log.Printf("Collection %s created", col.Name())
// 		}
// 	}

// 	// # Open a collection
// 	col, err := db.Collection(ctx, n.Collection)
// 	if err != nil {
// 		log.Fatalf("Failed to open collection: %v", err)
// 	}

// 	// # create a document
// 	doc := n.Data
// 	doc["ID"] = n.ID
// 	doc["Name"] = n.Name
// 	doc["Collection"] = n.Collection

// 	_, err = col.CreateDocument(ctx, doc)
// 	if err != nil {
// 		log.Fatalf("Failed to create document: %v", err)
// 	}

// 	return nil
// }

// // # AddEdge
// func (ag *ArangoGraph) AddEdge(e *base.EdgeJSON) error {
// 	// add edge to the arangodb
// 	ctx := context.Background()
// 	// # Open a database
// 	db, err := ag.Client.Database(ctx, ag.dbname)
// 	if err != nil {
// 		log.Fatalf("Failed to open database: %v", err)
// 	}

// 	// # check if the edge collection exists
// 	if exists, err := db.CollectionExists(ctx, e.Collection); err != nil {
// 		log.Fatalf("Failed to check for edge collection: %v", err)
// 	} else {
// 		if !exists {
// 			op := &driver.CreateCollectionOptions{
// 				Type: driver.CollectionTypeEdge,
// 			}

// 			// create an edge collection
// 			_, err := db.CreateCollection(ctx, e.Collection, op)
// 			if err != nil {
// 				log.Fatalf("Failed to create edge collection: %v", err)
// 			}
// 		}
// 	}

// 	// # Open an edge collection
// 	edgeCol, err := db.Collection(ctx, e.Collection)
// 	if err != nil {
// 		log.Fatalf("Failed to open edge collection: %v", err)
// 	}

// 	// # create a document
// 	doc := e.Data
// 	doc["_from"] = e.From
// 	doc["_to"] = e.To

// 	_, err = edgeCol.CreateDocument(ctx, doc)
// 	if err != nil {
// 		log.Fatalf("Failed to create document: %v", err)
// 	}

// 	return nil


// }



// // # AddGraph
// func (ag *ArangoGraph) AddGraph() error {
// 	// add graph to the arangodb
// 	ctx := context.Background()
// 	// # Open a database
// 	db, err := ag.Client.Database(ctx, ag.dbname)
// 	if err != nil {
// 		log.Fatalf("Failed to open database: %v", err)
// 	}

// 	// # check if the graph exists
// 	if exists, err := db.GraphExists(ctx, ag.Name); err != nil {
// 		log.Fatalf("Failed to check for graph: %v", err)
// 	} else {
// 		if !exists {
// 			op := &driver.CreateGraphOptions{
// 				EdgeDefinitions: []driver.EdgeDefinition{
// 					{
// 						Collection: "my_edge_collection", 
// 						From:       []string{"mycol"}, 
// 						To:         []string{"mycol"}},
// 				},
// 			}

// 			// create a graph
// 			_, err := db.CreateGraph(ctx, ag.Name,  op)
// 			if err != nil {
// 				log.Fatalf("Failed to create graph: %v", err)
// 			}
// 		}
// 	}

// 	// # Open a graph
// 	graph, err := db.Graph(ctx, ag.Name)
// 	// 4. Access the vertex and edge collections
//     vertexCol, err := graph.VertexCollection(context.Background(), "mycol")
//     if err != nil {
//         log.Fatalf("Failed to get vertex collection: %v", err)
//     }
//     edgeCol,_,err := graph.EdgeCollection(context.Background(), "my_edge_collection")
//     if err != nil {
//         log.Fatalf("Failed to get edge collection: %v", err)
//     }

//     // 5. Add 3 vertices (nodes) to the graph
//     vertex1 := map[string]interface{}{"name": "Node1"}
//     vertex2 := map[string]interface{}{"name": "Node2"}
//     vertex3 := map[string]interface{}{"name": "Node3"}

//     meta1, err := vertexCol.CreateDocument(context.Background(), vertex1)
//     if err != nil {
//         log.Fatalf("Failed to create vertex 1: %v", err)
//     }
//     fmt.Printf("Vertex 1 created with key: %s\n", meta1.Key)

//     meta2, err := vertexCol.CreateDocument(context.Background(), vertex2)
//     if err != nil {
//         log.Fatalf("Failed to create vertex 2: %v", err)
//     }
//     fmt.Printf("Vertex 2 created with key: %s\n", meta2.Key)

//     meta3, err := vertexCol.CreateDocument(context.Background(), vertex3)
//     if err != nil {
//         log.Fatalf("Failed to create vertex 3: %v", err)
//     }
//     fmt.Printf("Vertex 3 created with key: %s\n", meta3.Key)

//     // 6. Add 2 edges between the nodes
//     edge1 := map[string]interface{}{
//         "_from": "mycol/" + meta1.Key,
//         "_to":   "mycol/" + meta2.Key,
//         "relationship": "connected",
//     }
//     edgeMeta1, err := edgeCol.CreateDocument(context.Background(), edge1)
//     if err != nil {
//         log.Fatalf("Failed to create edge 1: %v", err)
//     }
//     fmt.Printf("Edge 1 created with key: %s\n", edgeMeta1.Key)

//     edge2 := map[string]interface{}{
//         "_from": "mycol/" + meta2.Key,
//         "_to":   "mycol/" + meta3.Key,
//         "relationship": "connected",
//     }
//     edgeMeta2, err := edgeCol.CreateDocument(context.Background(), edge2)
//     if err != nil {
//         log.Fatalf("Failed to create edge 2: %v", err)
//     }
//     fmt.Printf("Edge 2 created with key: %s\n", edgeMeta2.Key)
// 	return nil
// }

// // & AddFunc Section





// // func Init(yamlPath string) error {
// // 	// # Connect to ArangoDB


// // 	// # Open a database
// // 	ctx := context.Background()
// // 	db, err := c.Database(ctx, "_system")
// // 	if err != nil {
// // 		log.Fatalf("Failed to open database: %v", err)
// // 	}

// // 	fmt.Println(db.Name())

// // 	// # Create a database "mydb" with check
// // 	if exists, err := c.DatabaseExists(ctx, "mydb"); err != nil {
// // 		log.Fatalf("Failed to check for database: %v", err)
// // 	} else {
// // 		if exists {
// // 			fmt.Println("Database exists")
// // 		} else {
// // 			fmt.Println("Database does not exist")
// // 			// create a database
// // 			db, err = c.CreateDatabase(ctx, "mydb", nil)
// // 			if err != nil {
// // 				log.Fatalf("Failed to create database: %v", err)
// // 			}
// // 			fmt.Println(db.Name())
// // 		}
// // 	}

// // 	// # open the database
// // 	db, err = c.Database(ctx, "mydb")
// // 	if err != nil {
// // 		log.Fatalf("Failed to open database: %v", err)
// // 	}

// // 	// # create a collection "mycol" with check
// // 	if exists, err := db.CollectionExists(ctx, "mycol"); err != nil {
// // 		log.Fatalf("Failed to check for collection: %v", err)
// // 	} else {
// // 		if exists {
// // 			fmt.Println("Collection exists")
// // 		} else {
// // 			fmt.Println("Collection does not exist")
// // 			// create a collection
// // 			col, err := db.CreateCollection(ctx, "mycol", nil)
// // 			if err != nil {
// // 				log.Fatalf("Failed to create collection: %v", err)
// // 			}
// // 			fmt.Println(col.Name())
// // 		}
// // 	}

// // 	// # open a collection
// // 	col, err := db.Collection(ctx, "mycol")
// // 	if err != nil {
// // 		log.Fatalf("Failed to open collection: %v", err)
// // 	}

// // 	fmt.Println(col.Name())

// // 	// # create a document
// // 	doc := map[string]interface{}{
// // 		"key": "value",
// // 	}

// // 	meta, err := col.CreateDocument(ctx, doc)
// // 	if err != nil {
// // 		log.Fatalf("Failed to create document: %v", err)
// // 	}

// // 	fmt.Println(meta.Key)

// // 	// # check if a document exists
// // 	exists, err := col.DocumentExists(ctx, meta.Key)
// // 	if err != nil {
// // 		log.Fatalf("Failed to check for document: %v", err)
// // 	}
// // 	if exists {
// // 		fmt.Println("Document exists")
// // 	} else {
// // 		fmt.Println("Document does not exist")
// // 	}

// // 	// # read a document
// // 	var readDoc map[string]interface{}
// // 	meta, err = col.ReadDocument(ctx, meta.Key, &readDoc)
// // 	if err != nil {
// // 		log.Fatalf("Failed to read document: %v", err)
// // 	}

// // 	fmt.Println(meta.Key)

// // 	// # read a document with an explicit revision
// // 	type MyDocument struct {
// // 		Key string `json:"_key"`
// // 		Rev string `json:"_rev"`
// // 	}
// // 	var docc MyDocument

// // 	revCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// // 	defer cancel()

// // 	meta, err = col.ReadDocument(revCtx, meta.Key, &docc)
// // 	if err != nil {
// // 		// handle error
// // 		fmt.Println(err)
// // 	}

// // 	fmt.Println(meta.Key)

// // 	// remove the document
// // 	meta, err = col.RemoveDocument(ctx, meta.Key)
// // 	if err != nil {
// // 		log.Fatalf("Failed to remove document: %v", err)
// // 	}

// // 	fmt.Println(meta.Key)

// // 	// ctx := context.Background()
// // 	query := "FOR d IN mycol LIMIT 10 RETURN d"
// // 	cursor, err := db.Query(ctx, query, nil)
// // 	if err != nil {
// // 		// handle error
// // 	}
// // 	defer cursor.Close()
// // 	for {
// // 		var doc MyDocument
// // 		meta, err := cursor.ReadDocument(ctx, &doc)
// // 		if driver.IsNoMoreDocuments(err) {
// // 			break
// // 		} else if err != nil {
// // 			// handle other errors
// // 		}
// // 		fmt.Printf("Got doc with key '%s' from query\n", meta.Key)
// // 	}

// // 	ctx = driver.WithQueryCount(context.Background())
// // 	query = "FOR d IN mycol RETURN d"
// // 	cursor, err = db.Query(ctx, query, nil)
// // 	if err != nil {
// // 		// handle error
// // 	}
// // 	defer cursor.Close()
// // 	fmt.Printf("Query yields %d documents\n", cursor.Count())

// // 	ctx = driver.WithQueryCount(context.Background())
// // 	query = "FOR d IN mycol FILTER d._key == @myVar RETURN d"
// // 	bindVars := map[string]interface{}{
// // 		"myVar": "2255",
// // 	}
// // 	cursor, err = db.Query(ctx, query, bindVars)
// // 	if err != nil {
// // 		// handle error
// // 	}
// // 	defer cursor.Close()
// // 	fmt.Printf("Query yields %d documents\n", cursor.Count())

// // 	return nil

// // }
