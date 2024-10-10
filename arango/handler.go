package arango

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/emirpasic/gods/maps/hashbidimap"
	"github.com/wonderstone/chainstorm/handler"
	"github.com/wonderstone/chainstorm/tools"
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
	// graph part
	ag.graphname = data["graphname"].(string)
	// bidiMap
	ag.nodeNameToIDMap = hashbidimap.New()

	// logger
	loggerConfig := data["logger"].(map[string]interface{})
	logger := tools.NewLogger(loggerConfig)
	ag.logger = &logger

	// log out: say init success
	ag.logger.Info().Msgf("ArangoGraph initialized")

	return nil
}

// - Connection operations
// Connect() error
// + client and db fields are created
func (ag *ArangoGraph) Connect() error {
	ctx := context.Background()
	// connect to the arango database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{ag.server + ":" + strconv.Itoa((ag.port))},
	})
	if err != nil {
		ag.logger.Info().Msgf("Failed to create connection: %v", err)
	}

	ag.Client, err = driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(ag.username, ag.password),
	})
	if err != nil {
		ag.logger.Info().Msgf("Failed to create client: %v", err)
	}

	// get the database
	ag.db, err = ag.Client.Database(ctx, ag.dbname)

	if err != nil {
		return err
	}

	// list all the node collections
	collections, err := ag.db.Collections(ctx)
	if err != nil {
		ag.logger.Info().Msgf("Failed to list collections: %v", err)
		return err
	}

	// iterate over the collections and add the node names to the bidimap
	for _, col := range collections {

		props, err := col.Properties(ctx)
		if err != nil {
			ag.logger.Info().Msgf("Failed to get collection properties: %v", err)
			return err
		}

		// Skip system collections
		if props.IsSystem {
			continue
		}

		// Skip edge collections
		if props.Type == driver.CollectionTypeEdge {
			continue
		}

		// Create a unique index on the "name" field
		_, _, err = col.EnsurePersistentIndex(ctx, []string{"name"}, &driver.EnsurePersistentIndexOptions{
			Unique: true,
		})
		if err != nil {
			ag.logger.Info().Msgf("Failed to create index: %v", err)
			return err
		}

		query := fmt.Sprintf("FOR doc IN %s RETURN doc", col.Name())
		cursor, err := ag.db.Query(ctx, query, nil)
		if err != nil {
			ag.logger.Info().Msgf("Failed to execute query: %v",
				err)
			return err
		}
		defer cursor.Close()

		for {
			var doc Node
			_, err := cursor.ReadDocument(ctx, &doc)
			if driver.IsNoMoreDocuments(err) {
				break
			} else if err != nil {
				ag.logger.Info().Msgf("Failed to read document: %v",
					err)
				return err
			}
			// check if the doc.Name is already in the bidimap
			// if it is, then error
			if _, ok := ag.nodeNameToIDMap.Get(doc.Name); ok {
				ag.logger.Info().Msgf("Node %s already exists", doc.Name)
				return fmt.Errorf("node already exists")
			}
			// add the node name to the bidimap
			ag.nodeNameToIDMap.Put(doc.Name, doc.ID)
		}

	}

	// iter all the edge collections and check if the edge from and to exist
	for _, col := range collections {

		props, err := col.Properties(ctx)
		if err != nil {
			ag.logger.Info().Msgf("Failed to get collection properties: %v", err)
			return err
		}

		// Skip system collections
		if props.IsSystem {
			continue
		}

		// Skip node collections
		if props.Type == driver.CollectionTypeDocument {
			continue
		}

		query := fmt.Sprintf("FOR doc IN %s RETURN doc", col.Name())
		cursor, err := ag.db.Query(ctx, query, nil)
		if err != nil {
			ag.logger.Info().Msgf("Failed to execute query: %v",
				err)
			return err
		}
		defer cursor.Close()

		for {
			var doc Edge
			_, err := cursor.ReadDocument(ctx, &doc)
			if driver.IsNoMoreDocuments(err) {
				break
			} else if err != nil {
				ag.logger.Info().Msgf("Failed to read document: %v",
					err)
				return err
			}
			// check if the doc.From and doc.To are already in the bidimap
			// if it is, then error
			if _, ok := ag.nodeNameToIDMap.GetKey(doc.From); !ok {
				ag.logger.Info().Msgf("Node %s does not exist", doc.From)
				return fmt.Errorf("node does not exist")
			}
			if _, ok := ag.nodeNameToIDMap.GetKey(doc.To); !ok {
				ag.logger.Info().Msgf("Node %s does not exist", doc.To)
				return fmt.Errorf("node does not exist")
			}
		}
	}

	// output the log that the connection is successful
	ag.logger.Info().Msgf("Connected to ArangoDB")

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

// todo: createGraph
func (ag *ArangoGraph) createGraph() error {
	// get all the collections

	ctx := context.Background()
	collections, err := ag.db.Collections(ctx)
	if err != nil {
		ag.logger.Info().Msgf("Failed to list collections: %v", err)
		return err
	}

	// iter all the collections and output the non-system collections to nodeCollections and edgeCollections
	var nodeCollections []string
	var edgeCollections []string
	for _, col := range collections {
		props, err := col.Properties(ctx)
		if err != nil {
			ag.logger.Info().Msgf("Failed to get collection properties: %v", err)
			return err
		}

		// Skip system collections
		if props.IsSystem {
			continue
		}

		// Skip edge collections
		if props.Type == driver.CollectionTypeEdge {
			edgeCollections = append(edgeCollections, col.Name())
			continue
		}

		nodeCollections = append(nodeCollections, col.Name())
	}

	// create the []driver.EdgeDefinition slice
	var edgeDefinitions []driver.EdgeDefinition
	for _, col := range edgeCollections {
		edgeDefinitions = append(edgeDefinitions, driver.EdgeDefinition{
			Collection: col,
			From:       nodeCollections,
			To:         nodeCollections,
		})
	}

	options := driver.CreateGraphOptions{
		EdgeDefinitions: edgeDefinitions,
	}

	// create the graph
	ag.graph, err = ag.db.CreateGraphV2(context.TODO(), ag.graphname, &options)
	// ag.graph, err = ag.db.CreateGraphV2(context.TODO(), ag.graphname, nil)

	if err != nil {
		return err
	}

	return nil
}

// - CRUD operations
// + Create operations

// checkItemExists checks if the node exists
func (ag *ArangoGraph) checkItemExists(id string) (bool, error) {
	ctx := context.Background()
	// split the id into collection and name
	infos := strings.Split(id, "/")
	if len(infos) != 2 {
		ag.logger.Info().Msgf("Invalid id: %s", id)
		return false, fmt.Errorf("invalid id")
	}

	// check if the collection exists
	if exists, err := ag.db.CollectionExists(ctx, infos[0]); err != nil {
		ag.logger.Info().Msgf("Failed to check for collection: %v", err)
		return false, err
	} else {
		if !exists {
			ag.logger.Info().Msgf("Collection %s does not exist", infos[0])
			return false, fmt.Errorf("collection %s does not exist", infos[0])
		}
	}

	// check if the document exists
	col, err := ag.db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Info().Msgf("Failed to open collection: %v", err)
		return false, err
	}

	if exists, err := col.DocumentExists(ctx, infos[1]); err != nil {
		ag.logger.Info().Msgf("Failed to check for document: %v", err)
		return false, err
	} else {
		if exists {
			ag.logger.Info().Msgf("Document %s exists", id)
			return true, nil
		}
	}
	ag.logger.Info().Msgf("Document %s does not exist", id)
	return false, nil
}

// node name should be unique
func (ag *ArangoGraph) AddNode(ni handler.Node) (interface{}, error) {
	// convert ni to Node
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
		ag.logger.Info().Msgf("Failed to open database: %v", err)
		return nil, err
	}
	// # check if the collection exists
	if exists, err := db.CollectionExists(ctx, n.Collection); err != nil {
		ag.logger.Info().Msgf("Failed to check for collection: %v", err)
		return nil, err
	} else {
		if !exists {
			// create a collection
			col, err := db.CreateCollection(ctx, n.Collection, nil)
			if err != nil {
				ag.logger.Info().Msgf("Failed to create collection: %v", err)
				return nil, err
			}
			ag.logger.Info().Msgf("Collection %s created", col.Name())
			// Create a unique index on the "name" field
			_, _, err = col.EnsurePersistentIndex(ctx, []string{"name"}, &driver.EnsurePersistentIndexOptions{
				Unique: true,
			})
			if err != nil {
				ag.logger.Info().Msgf("Failed to create index: %v", err)
				return nil,err
			}
		}
	}

	// # Open a collection
	col, err := db.Collection(ctx, n.Collection)
	if err != nil {
		ag.logger.Info().Msgf("Failed to open collection: %v", err)
		return nil, err
	}

	// # check if some document with the same name exists
	// # if it exists, return an error
	if exists, err := col.DocumentExists(ctx, n.Name); err != nil {
		ag.logger.Info().Msgf("Failed to check for document: %v", err)
		return nil, err
	} else {
		if exists {
			ag.logger.Info().Msgf("Document %s already exists", n.Name)
			return nil, fmt.Errorf("document already exists")
		}
	}

	// # create a document
	doc := make(map[string]interface{})
	doc["data"] = n.Data
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	meta, err := col.CreateDocument(ctx, doc)
	if err != nil {
		ag.logger.Info().Msgf("Failed to create document: %v", err)
		return nil, err
	}

	// # add the node info to the bidirectional map nodeNameToIDMap
	// # bidiMap itself do not check if the key already exists
	// # it is the arangodb's former operations that check if the document already exists
	// # bidiMap actually will replace the old value with the new value
	ag.nodeNameToIDMap.Put(n.Name, meta.ID)
	return meta, nil
}

// AddEdge(e Edge) (interface{}, error)
func (ag *ArangoGraph) AddEdge(ei handler.Edge) (interface{}, error) {
	ctx := context.Background()
	// convert the handler.Edge to Edge
	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		ag.logger.Info().Msgf("Invalid input")
		return nil, fmt.Errorf("invalid input")
	}

	// # check if the collection exists
	if exists, err := ag.db.CollectionExists(ctx, e.Collection); err != nil {
		ag.logger.Info().Msgf("Failed to check for collection: %v", err)
		return nil, err
	} else {
		if !exists {
			// create an edge collection
			col, err := ag.db.CreateCollection(ctx, e.Collection, &driver.CreateCollectionOptions{
				Type: driver.CollectionTypeEdge,
			})
			if err != nil {
				ag.logger.Info().Msgf("Failed to create collection: %v", err)
				return nil, err
			}
			ag.logger.Info().Msgf("Collection %s created", col.Name())
		}
	}

	// # open the edge collection
	// & not using the ag.db but defining the db again
	// & in case the ag is not connected yet
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Info().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	edgeCol, err := db.Collection(ctx, e.Collection)
	if err != nil {
		ag.logger.Info().Msgf("Failed to open edge collection: %v", err)
		return nil, err
	}
	doc := make(map[string]interface{})
	doc["data"] = e.Data
	doc["_id"] = e.ID
	// # check if the from and to nodes exist using checkNodeExists
	exists, err := ag.checkItemExists(e.From)
	if err != nil {
		ag.logger.Info().Msgf("Failed to check for from node: %v", err)
		return nil, err
	}
	if !exists {
		ag.logger.Info().Msgf("from node %s not exists", e.From)
		return nil, fmt.Errorf("from node not exists")
	}

	exists, err = ag.checkItemExists(e.To)
	if err != nil {
		ag.logger.Info().Msgf("Failed to check for to node: %v", err)
		return nil, err
	}

	if !exists {
		ag.logger.Info().Msgf("to node %s not exists", e.To)
		return nil, fmt.Errorf("to node not exists")
	}
	// # add the from and to nodes to the edge document
	// # add the collection and relationship to the edge document
	doc["_from"] = e.From
	doc["_to"] = e.To
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship

	meta, err := edgeCol.CreateDocument(ctx, doc)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

// + Update operations
// - ReplaceNode(n Node) error
func (ag *ArangoGraph) ReplaceNode(ni handler.Node) error {
	// convert ni to Node
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// # get the id from the bidimap
	// % if the id is blank, assign the id from the bidimap
	// % if the id is not blank, check if they are the same, if not, return an error
	id, ok := ag.nodeNameToIDMap.Get(n.Name)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.Name)
		return fmt.Errorf("node does not exist")
	}

	if n.ID != "" {
		if n.ID != id.(driver.DocumentID).String() {
			ag.logger.Fatal().Msgf("ID %s does not match the ID in the bidimap %s", n.ID, id.(driver.DocumentID).String())
			return fmt.Errorf("id does not match")
		}
	} else {
		n.ID = id.(driver.DocumentID).String()
	}

	// # get the collection and key from the id
	infos := strings.Split(n.ID, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", n.ID)
		return fmt.Errorf("invalid id")
	}

	// # check if the node exists
	exists, err := ag.checkItemExists(n.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.ID)
		return fmt.Errorf("node does not exist")
	}

	// # replace the node
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}
	// % create the doc for replacment
	doc := make(map[string]interface{})
	doc["data"] = n.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection
	// % replace the document
	_, err = col.ReplaceDocument(ctx, infos[1], doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to replace document: %v", err)
		return err
	}
	return nil
}

// ReplaceEdge(e Edge) error
func (ag *ArangoGraph) ReplaceEdge(ei handler.Edge) error {
	// convert the handler.Edge to Edge
	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		ag.logger.Fatal().Msgf("Invalid input")
		return fmt.Errorf("invalid input")
	}
	// = For edge, must have the id
	// = if the id is blank, return an error
	// todo: get it from the GetEdgesByRegex method
	if e.ID == "" {
		ag.logger.Fatal().Msgf("Edge id is blank")
		return fmt.Errorf("edge id is blank")
	}


	// # check if the edge exists
	exists, err := ag.checkItemExists(e.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for edge: %v", err)
		return err
	}

	if !exists {
		ag.logger.Fatal().Msgf("Edge %s does not exist", e.ID)
		return fmt.Errorf("edge does not exist")
	}

	// # get the collection and key from the id
	infos := strings.Split(e.ID, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", e.ID)
		return fmt.Errorf("invalid id")
	}

	// % replace the edge
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, e.Collection)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}
	// % create the doc for replacment
	doc := make(map[string]interface{})
	doc["data"] = e.Data
	doc["_id"] = e.ID
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship
	doc["_from"] = e.From
	doc["_to"] = e.To

	// % replace the document
	_, err = col.ReplaceDocument(ctx, infos[1], doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to replace document: %v", err)
		return err
	}

	return nil
}

// UpdateNode(n Node) error
// - Only the specified fields in the update document are modified. 
// - Fields that are not specified in the update document remain unchanged.

func (ag *ArangoGraph) UpdateNode(ni handler.Node) error {
	// convert ni to Node
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// # get the id from the bidimap
	// % if the id is blank, assign the id from the bidimap
	// % if the id is not blank, check if they are the same, if not, return an error
	id, ok := ag.nodeNameToIDMap.Get(n.Name)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.Name)
		return fmt.Errorf("node does not exist")
	}

	if n.ID != "" {
		if n.ID != id.(driver.DocumentID).String() {
			ag.logger.Fatal().Msgf("ID %s does not match the ID in the bidimap %s", n.ID, id.(driver.DocumentID).String())
			return fmt.Errorf("id does not match")
		}
	} else {
		n.ID = id.(driver.DocumentID).String()
	}

	// # get the collection and key from the id
	infos := strings.Split(n.ID, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", n.ID)
		return fmt.Errorf("invalid id")
	}

	// # check if the node exists
	exists, err := ag.checkItemExists(n.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v",err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.ID)
		return fmt.Errorf("node does not exist")
	}

	// # update the node
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v",err)
		return err
	}

	col, err := db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v",	err)
		return err
	}
	// % create the doc for update
	doc := make(map[string]interface{})
	doc["data"] = n.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection
	// % update the document
	_, err = col.UpdateDocument(ctx, infos[1], doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}
	return nil
}

// UpdateEdge(e Edge) error
func (ag *ArangoGraph) UpdateEdge(ei handler.Edge) error {
	// convert the handler.Edge to Edge
	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		ag.logger.Fatal().Msgf("Invalid input")
		return fmt.Errorf("invalid input")
	}

	// = For edge, must have the id
	// = if the id is blank, return an error
	// todo: get it from the GetEdgesByRegex method
	if e.ID == "" {
		ag.logger.Fatal().Msgf("Edge id is blank")
		return fmt.Errorf("edge id is blank")
	}

	// # check if the edge exists
	exists, err := ag.checkItemExists(e.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for edge: %v", err)
		return err
	}

	if !exists {
		ag.logger.Fatal().Msgf("Edge %s does not exist", e.ID)
		return fmt.Errorf("edge does not exist")
	}

	// # get the collection and key from the id
	infos := strings.Split(e.ID, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", e.ID)
		return fmt.Errorf("invalid id")
	}

	// % update the edge
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}
	// % create the doc for update
	doc := make(map[string]interface{})
	doc["data"] = e.Data
	doc["_id"] = e.ID
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship
	doc["_from"] = e.From
	doc["_to"] = e.To
	// % update the document
	_, err = col.UpdateDocument(ctx, infos[1], doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}
	return nil
}

// MergeNode(n Node) error
// - The same fields in the document are added together

func (ag *ArangoGraph) MergeNode(ni handler.Node) error {
	// convert ni to Node
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// # get the id from the bidimap
	// % if the id is blank, assign the id from the bidimap
	// % if the id is not blank, check if they are the same, if not, return an error
	id, ok := ag.nodeNameToIDMap.Get(n.Name)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.Name)
		return fmt.Errorf("node does not exist")
	}

	if n.ID != "" {
		if n.ID != id.(driver.DocumentID).String() {
			ag.logger.Fatal().Msgf("ID %s does not match the ID in the bidimap %s", n.ID, id.(driver.DocumentID).String())
			return fmt.Errorf("id does not match")
		}
	} else {
		n.ID = id.(driver.DocumentID).String()
	}

	// # get the collection and key from the id
	infos := strings.Split(n.ID, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", n.ID)
		return fmt.Errorf("invalid id")
	}

	// # check if the node exists
	exists, err := ag.checkItemExists(n.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.ID)
		return fmt.Errorf("node does not exist")
	}

	// # replace the node
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}

	// # get the old data
	var oldNode Node
	_, err = col.ReadDocument(ctx, infos[1], &oldNode)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to read document: %v", err)
		return err
	}

	// # merge the data
	// $ if the data is new, add it to the oldNode
	// $ if the data is not new, oldNode same fields are added together
	for k, v := range n.Data {
		if _, ok := oldNode.Data[k]; !ok {
			oldNode.Data[k] = v
		} else {
			if v != "" {
				// type assertion
				switch v.(type) {
				case string:
					oldNode.Data[k] = oldNode.Data[k].(string) + v.(string)
				case int:
					oldNode.Data[k] = oldNode.Data[k].(int) + v.(int)
				case float64:
					oldNode.Data[k] = oldNode.Data[k].(float64) + v.(float64)
				case bool:
					oldNode.Data[k] = oldNode.Data[k].(bool) || v.(bool)
				default:
					ag.logger.Fatal().Msgf("Invalid data type")
				}
			}
		}
	}

	// update the node
	doc := make(map[string]interface{})
	doc["data"] = oldNode.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	_, err = col.UpdateDocument(ctx, infos[1], doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}

	return nil

}

// MergeEdge(e Edge) error
func (ag *ArangoGraph) MergeEdge(ei handler.Edge) error {
	// convert the handler.Edge to Edge
	var e Edge
	switch v := ei.(type) {
	case *Edge:
		e = *v
	default:
		ag.logger.Fatal().Msgf("Invalid input")
		return fmt.Errorf("invalid input")
	}

	// = For edge, must have the id
	// = if the id is blank, return an error
	if e.ID == "" {
		ag.logger.Fatal().Msgf("Edge id is blank")
		return fmt.Errorf("edge id is blank")
	}

	// # check if the edge exists
	exists, err := ag.checkItemExists(e.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for edge: %v",err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Edge %s does not exist", e.ID)
		return fmt.Errorf("edge does not exist")
	}

	// # get the collection and key from the id
	infos := strings.Split(e.ID, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", e.ID)
		return fmt.Errorf("invalid id")
	}

	// # update the edge
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v",err)
		return err
	}

	col, err := db.Collection(ctx, infos[0])

	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v",err)
		return err
	}

	// # get the old data
	var oldEdge Edge
	_, err = col.ReadDocument(ctx, infos[1], &oldEdge)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to read document: %v", err)
		return err
	}

	// # merge the data
	// $ if the data is new, add it to the oldEdge
	// $ if the data is not new, oldEdge same fields are added together
	for k, v := range e.Data {
		if _, ok := oldEdge.Data[k]; !ok {
			oldEdge.Data[k] = v
		} else {
			if v != "" {
				// type assertion
				switch v.(type) {
				case string:
					oldEdge.Data[k] = oldEdge.Data[k].(string) + v.(string)
				case int:
					oldEdge.Data[k] = oldEdge.Data[k].(int) + v.(int)
				case float64:
					oldEdge.Data[k] = oldEdge.Data[k].(float64) + v.(float64)
				case bool:
					oldEdge.Data[k] = oldEdge.Data[k].(bool) || v.(bool)
				default:
					ag.logger.Fatal().Msgf("Invalid data type")
				}
			}
		}
	}

	// update the edge
	doc := make(map[string]interface{})
	doc["data"] = oldEdge.Data
	doc["_id"] = e.ID
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship
	doc["_from"] = e.From
	doc["_to"] = e.To

	_, err = col.UpdateDocument(ctx, infos[1], doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}

	return nil

}

// + Delete operations
// DeleteNode(name interface{}) error
func (ag *ArangoGraph) DeleteNode(name interface{}) error {
	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(name)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", name)
		return fmt.Errorf("node does not exist")
	}

	// delete the document by _id
	err := ag.DeleteItemByID(id)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to delete node: %v", err)
		return err
	}

	// remove the node from the bidimap
	ag.nodeNameToIDMap.Remove(name)

	return nil

}

// DeleteItemByID(id interface{}) error
func (ag *ArangoGraph) DeleteItemByID(id interface{}) error {
	ctx := context.Background()

	// convert id to string
	idStr, ok := id.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid id: %v", id)
		return fmt.Errorf("invalid id")
	}

	// split the id into collection and name
	infos := strings.Split(idStr, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", idStr)
		return fmt.Errorf("invalid id")
	}

	// get the collection
	col, err := ag.db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}

	// delete the document by _id
	_, err = col.RemoveDocument(ctx, infos[1])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to delete document: %v", err)
		return err
	}

	// remove the node from the bidimap
	// bidimap name is the name, value is the id
	// so we need to to get the name first
	// then remove the name which is the name
	// then remove the value which is the id
	name, ok := ag.nodeNameToIDMap.GetKey(idStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", idStr)
		return fmt.Errorf("node does not exist")
	}
	ag.nodeNameToIDMap.Remove(name)

	return nil
}

// + Query operations
// GetItemByID(id interface{}) (interface{}, error)
func (ag *ArangoGraph) GetItemByID(id interface{}) (interface{}, error) {
	ctx := context.Background()
	// type assertion to check if the id is a string
	var idStr string
	switch id := id.(type) {
	case string:
		idStr = id
	case driver.DocumentID:
		idStr = id.String()
	default:
		ag.logger.Fatal().Msgf("Invalid id: %v", id)
		return nil, fmt.Errorf("invalid id")
	}

	// split the id into collection and name
	infos := strings.Split(idStr, "/")
	if len(infos) != 2 {
		ag.logger.Fatal().Msgf("Invalid id: %s", idStr)
		return nil, fmt.Errorf("invalid id")
	}

	// get the collection
	col, err := ag.db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return nil, err
	}

	// check the collection type
	// if it is a node collection, return node
	// if it is an edge collection, return edge
	props, err := col.Properties(ctx)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to get collection properties: %v", err)
		return nil, err
	}

	switch props.Type {
	case driver.CollectionTypeDocument:
		var doc Node
		_, err = col.ReadDocument(ctx, infos[1], &doc)
		if err != nil {
			ag.logger.Fatal().Msgf("Failed to get document: %v", err)
			return nil, err
		}
		return doc, nil
	case driver.CollectionTypeEdge:
		var edge Edge
		_, err = col.ReadDocument(ctx, infos[1], &edge)
		if err != nil {
			ag.logger.Fatal().Msgf("Failed to get document: %v", err)
			return nil, err
		}
		return edge, nil
	default:
		ag.logger.Fatal().Msgf("Invalid collection type: %v", props.Type)
		return nil, fmt.Errorf("invalid collection type")
	}
}






// GetNode(name interface{}) (Node, error)
func (ag *ArangoGraph) GetNode(name interface{}) (handler.Node, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the node by id using GetItemByID
	node, err := ag.GetItemByID(id)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to get node: %v", err)
		return nil, err
	}

	n, ok := node.(Node)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid node: %v", node)
		return nil, fmt.Errorf("invalid node")
	}

	return &n, nil
}

// Query method returns []interface{}, error
// interface{} is in the map[string]interface{} format
func (ag *ArangoGraph) Query(query string, bindVars map[string]interface{}) ([]interface{}, error) {
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var result []interface{}
	for {
		var doc interface{}
		_, err := cursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}
		result = append(result, doc)
	}

	return result, nil
}

// give a flexible query generator
func QueryGenerator(q interface{}) (string, map[string]interface{}, error) {
	var subqueries []string
	// # get collection
	cols, ok := q.(map[string]interface{})["collections"].([]string)
	if !ok {
		return "", nil, fmt.Errorf("collection not found")
	}

	// # get regexPatterns
	bindVars := make(map[string]interface{})
	regexPatterns, ok := q.(map[string]interface{})["regexPatterns"].([]string)
	if !ok {
		return "", nil, fmt.Errorf("regex not found")
	}
	for i, regex := range regexPatterns {
		regexVar := fmt.Sprintf("regex%d", i)
		bindVars[regexVar] = regex
	}

	// # get filter
	filter, ok := q.(map[string]interface{})["filter"].(string)
	if !ok {
		return "", nil, fmt.Errorf("filter not found")
	}

	// # get base query
	query_base, ok := q.(map[string]interface{})["query"].(string)
	if !ok {
		return "", nil, fmt.Errorf("query not found")
	}

	for col := range cols {
		// Add subquery for the current collection
		subquery := fmt.Sprintf(query_base, cols[col], filter)
		subqueries = append(subqueries, subquery)
	}
	// # Combine subqueries using UNION
	query := fmt.Sprintf(`
        FOR node IN UNION(
            %s
        )
        RETURN node
    `, strings.Join(subqueries, ","))

	return query, bindVars, nil
}

func mapToStruct(m map[string]interface{}, result interface{}) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

// GetNodesByRegex(regex string) ([]Node, error)
func (ag *ArangoGraph) GetNodesByRegex(regex string) ([]handler.Node, error) {
	ctx := context.Background()
	// list all the node collections
	collections, err := ag.db.Collections(ctx)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to list collections: %v", err)
		return nil, err
	}

	// get all node collections
	var nodeCollections []string
	for _, col := range collections {
		props, err := col.Properties(ctx)
		if err != nil {
			ag.logger.Info().Msgf("Failed to get collection properties: %v", err)
			return nil, err
		}

		// Skip system collections
		if props.IsSystem {
			continue
		}

		// Skip edge collections
		if props.Type == driver.CollectionTypeEdge {
			continue
		}

		nodeCollections = append(nodeCollections, col.Name())
	}

	q := map[string]interface{}{
		// #
		"collections":   nodeCollections,
		"regexPatterns": []string{regex},
		// "filter": "REGEX_MATCHES(node.name, @regex0, true)",
		"filter": `
            REGEX_MATCHES(node.name, @regex0, true) 
        `,
		"query": `
			FOR node IN %s
			FILTER %s
			RETURN node
		`,
	}

	query, bindVars, err := QueryGenerator(q)

	if err != nil {
		return nil, err
	}

	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var nodes []*Node
	for {
		var node Node
		_, err := cursor.ReadDocument(ctx, &node)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}
		nodes = append(nodes, &node)
	}

	// Convert []*Node to []handler.Node
	handlerNodes := make([]handler.Node, len(nodes))
	for i, n := range nodes {
		handlerNodes[i] = n
	}

	return handlerNodes, nil
}

// GetEdgesByRegex(regex string) ([]Edge, error)
func (ag *ArangoGraph) GetEdgesByRegex(regex string) ([]handler.Edge, error) {
	ctx := context.Background()
	// list all the edge collections
	collections, err := ag.db.Collections(ctx)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to list collections: %v", err)
		return nil, err
	}

	// get all edge collections
	var edgeCollections []string
	for _, col := range collections {
		props, err := col.Properties(ctx)
		if err != nil {
			ag.logger.Info().Msgf("Failed to get collection properties: %v", err)
			return nil, err
		}

		// Skip system collections
		if props.IsSystem {
			continue
		}

		// Skip node collections
		if props.Type == driver.CollectionTypeDocument {
			continue
		}

		edgeCollections = append(edgeCollections, col.Name())
	}

	q := map[string]interface{}{
		// #
		"collections":   edgeCollections,
		"regexPatterns": []string{regex},
		// "filter": "REGEX_MATCHES(node.name, @regex0, true)",
		"filter": `
			LENGTH(edge.relationship) > 0 AND 
			REGEX_MATCHES(edge.relationship, @regex0, true) 
		`,
		"query": `
			FOR edge IN %s
			FILTER %s
			RETURN edge
		`,
	}

	query, bindVars, err := QueryGenerator(q)

	if err != nil {
		return nil, err
	}

	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}

	defer cursor.Close()

	var edges []*Edge
	for {
		var edge Edge
		_, err := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}
		edges = append(edges, &edge)
	}

	// Convert []*Edge to []handler.Edge
	handlerEdges := make([]handler.Edge, len(edges))
	for i, e := range edges {
		handlerEdges[i] = e
	}

	return handlerEdges, nil

}

// GetFromNodes(name interface{}) ([]Node, error)
func (ag *ArangoGraph) GetFromNodes(name interface{}) ([]handler.Node, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	// Retrieve the list of collections
	collections, err := ag.db.Collections(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %v", err)
	}

	var subqueries []string
	for _, col := range collections {
		props, err := col.Properties(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get collection properties: %v", err)
		}

		// Check if the collection is an edge collection
		// Check if the collection is system collection
		if props.Type == driver.CollectionTypeEdge && !props.IsSystem {
			// Add subquery for the current edge collection
			subquery := fmt.Sprintf(`
                FOR edge IN %s
                FILTER edge._to == @id
                RETURN edge._from
            `, col.Name())
			subqueries = append(subqueries, subquery)
		}
	}

	// Combine subqueries using UNION
	query := fmt.Sprintf(`
        FOR edge IN UNION(
            %s
        )
        RETURN edge
    `, strings.Join(subqueries, ","))

	bindVars := map[string]interface{}{
		"id": id,
	}

	cursor, err := ag.db.Query(ctx, query, bindVars)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer cursor.Close()

	var fromNodes []handler.Node
	for {
		var from string
		_, err := cursor.ReadDocument(ctx, &from)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read document: %v", err)
		}
		// get the node
		node, err := ag.GetItemByID(from)
		if err != nil {
			return nil, fmt.Errorf("failed to get node: %v", err)
		}

		n, ok := node.(Node)
		if !ok {
			return nil, fmt.Errorf("invalid node")
		}
		fromNodes = append(fromNodes, &n)
	}

	return fromNodes, nil
}

// GetToNodes(name interface{}) ([]Node, error)
func (ag *ArangoGraph) GetToNodes(name interface{}) ([]handler.Node, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR edge IN edges
	FILTER edge._from == @id
	RETURN edge
	`
	bindVars := map[string]interface{}{
		"id": id,
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var nodes []handler.Node
	for {
		var edge Edge
		_, err := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}

		// get the node
		node, err := ag.GetItemByID(edge.To)
		if err != nil {
			ag.logger.Fatal().Msgf("Failed to get node: %v", err)
			return nil, err
		}

		n, ok := node.(handler.Node)
		if !ok {
			ag.logger.Fatal().Msgf("Invalid node: %v", node)
			return nil, fmt.Errorf("invalid node")
		}
		nodes = append(nodes, n)
	}

	return nodes, nil
}

// GetInEdges(name interface{}) ([]Edge, error)
func (ag *ArangoGraph) GetInEdges(name interface{}) ([]handler.Edge, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR edge IN edges
	FILTER edge._to == @id
	RETURN edge
	`
	bindVars := map[string]interface{}{
		"id": id,
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var edges []handler.Edge
	for {
		var edge handler.Edge
		_, err := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}
		edges = append(edges, edge)
	}

	return edges, nil
}

// GetOutEdges(name interface{}) ([]Edge, error)
func (ag *ArangoGraph) GetOutEdges(name interface{}) ([]handler.Edge, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR edge IN edges
	FILTER edge._from == @id
	RETURN edge
	`
	bindVars := map[string]interface{}{
		"id": id,
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var edges []handler.Edge
	for {
		var edge handler.Edge
		_, err := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}
		edges = append(edges, edge)
	}

	return edges, nil
}

// + Graph operations
// - Traversal operations
// GetAllRelatedNodes(name interface{}) ([][]Node, error)
func (ag *ArangoGraph) GetAllRelatedNodes(name interface{}) ([][]handler.Node, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR edge IN edges
	FILTER edge._from == @id
	RETURN edge
	`
	bindVars := map[string]interface{}{
		"id": id,
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var nodes [][]handler.Node
	for {
		var edge Edge
		_, err := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}

		// get the node
		node, err := ag.GetItemByID(edge.To)
		if err != nil {
			ag.logger.Fatal().Msgf("Failed to get node: %v", err)
			return nil, err
		}

		n, ok := node.(handler.Node)
		if !ok {
			ag.logger.Fatal().Msgf("Invalid node: %v", node)
			return nil, fmt.Errorf("invalid node")
		}
		nodes = append(nodes, []handler.Node{n})
	}

	return nodes, nil
}

// GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...Edge) ([][]Node, error)
func (ag *ArangoGraph) GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...handler.Edge) ([][]handler.Node, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	_, ok = ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	_, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	// get the nodes
	var nodes [][]handler.Node
	for _, edge := range EdgeSlice {

		// convert the handler.Edge to Edge
		var e Edge
		switch v := edge.(type) {
		case *Edge:
			e = *v
		default:
			ag.logger.Fatal().Msgf("Invalid input")
			return nil, fmt.Errorf("invalid input")
		}

		// get the node

		node, err := ag.GetItemByID(e.To)
		if err != nil {
			ag.logger.Fatal().Msgf("Failed to get node: %v", err)
			return nil, err
		}

		n, ok := node.(handler.Node)
		if !ok {
			ag.logger.Fatal().Msgf("Invalid node: %v", node)
			return nil, fmt.Errorf("invalid node")
		}
		nodes = append(nodes, []handler.Node{n})
	}

	return nodes, nil
}

// GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)
func (ag *ArangoGraph) GetAllRelatedNodesInRange(name interface{}, max int) ([][]handler.Node, error) {
	// convert the name into string
	nameStr, ok := name.(string)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid name: %v", name)
		return nil, fmt.Errorf("invalid name")
	}

	// get the id from the bidimap with the name
	id, ok := ag.nodeNameToIDMap.Get(nameStr)
	if !ok {
		ag.logger.Fatal().Msgf("Node %s does not exist", nameStr)
		return nil, fmt.Errorf("node does not exist")
	}

	// get the edges from the edge collection
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR edge IN edges
	FILTER edge._from == @id
	LIMIT @max
	RETURN edge
	`
	bindVars := map[string]interface{}{
		"id":  id,
		"max": max,
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var nodes [][]handler.Node
	for {
		var edge Edge
		_, err := cursor.ReadDocument(ctx, &edge)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}

		// get the node
		node, err := ag.GetItemByID(edge.To)
		if err != nil {
			ag.logger.Fatal().Msgf("Failed to get node: %v", err)
			return nil, err
		}

		n, ok := node.(handler.Node)
		if !ok {
			ag.logger.Fatal().Msgf("Invalid node: %v", node)
			return nil, fmt.Errorf("invalid node")
		}
		nodes = append(nodes, []handler.Node{n})
	}

	return nodes, nil
}
