package arango

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
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
	// connect to the arango database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{ag.server + ":" + strconv.Itoa((ag.port))},
	})
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to create connection: %v", err)
	}

	ag.Client, err = driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(ag.username, ag.password),
	})
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to create client: %v", err)
	}

	// get the database
	ag.db, err = ag.Client.Database(context.TODO(), ag.dbname)

	if err != nil {
		return err
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
	ag.graph, err = ag.db.CreateGraphV2(context.TODO(), ag.graphname, &options)

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
		ag.logger.Fatal().Msgf("Invalid id: %s", id)
		return false, fmt.Errorf("invalid id")
	}

	// check if the collection exists
	if exists, err := ag.db.CollectionExists(ctx, infos[0]); err != nil {
		ag.logger.Fatal().Msgf("Failed to check for collection: %v", err)
		return false, err
	} else {
		if !exists {
			ag.logger.Fatal().Msgf("Collection %s does not exist", infos[0])
			return false, fmt.Errorf("collection does not exist")
		}
	}

	// check if the document exists
	col, err := ag.db.Collection(ctx, infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return false, err
	}

	if exists, err := col.DocumentExists(ctx, id); err != nil {
		ag.logger.Fatal().Msgf("Failed to check for document: %v", err)
		return false, err
	} else {
		if exists {
			ag.logger.Fatal().Msgf("Document %s already exists", id)
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
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}
	// # check if the collection exists
	if exists, err := db.CollectionExists(ctx, n.Collection); err != nil {
		ag.logger.Fatal().Msgf("Failed to check for collection: %v", err)
		return nil, err
	} else {
		if !exists {
			// create a collection
			col, err := db.CreateCollection(ctx, n.Collection, nil)
			if err != nil {
				ag.logger.Fatal().Msgf("Failed to create collection: %v", err)
				return nil, err
			}
			ag.logger.Info().Msgf("Collection %s created", col.Name())
		}
	}

	// # Open a collection
	col, err := db.Collection(ctx, n.Collection)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return nil, err
	}

	// # check if some document with the same name exists
	// # if it exists, return an error
	if exists, err := col.DocumentExists(ctx, n.Name); err != nil {
		ag.logger.Fatal().Msgf("Failed to check for document: %v", err)
		return nil, err
	} else {
		if exists {
			ag.logger.Fatal().Msgf("Document %s already exists", n.Name)
			return nil, fmt.Errorf("document already exists")
		}
	}

	// # create a document
	doc := n.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	meta, err := col.CreateDocument(ctx, doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to create document: %v", err)
		return nil, err
	}

	// # add the node info to the bidirectional map nodeNameToIDMap
	// # bidiMap itself do not check if the key already exists
	// # it is the arangodb's former operations that check if the document already exists
	// # bidiMap actually will replace the old value with the new value
	ag.nodeNameToIDMap.Put(n.Name, n.ID)
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
		ag.logger.Fatal().Msgf("Invalid input")
		return nil, fmt.Errorf("invalid input")
	}

	ctx := context.Background()

	// open the edge collection
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	edgeCol, err := db.Collection(ctx, e.Collection)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open edge collection: %v", err)
		return nil, err
	}

	doc := e.Data
	doc["_id"] = e.ID
	// check if the from and to nodes exist using checkNodeExists
	exists, err := ag.checkItemExists(e.From)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return nil, err
	}
	if exists {
		ag.logger.Fatal().Msgf("from node %s already exists", e.From)
		return nil, fmt.Errorf("from node already exists")
	}

	exists, err = ag.checkItemExists(e.To)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return nil, err
	}

	if exists {
		ag.logger.Fatal().Msgf("to node %s already exists", e.To)
		return nil, fmt.Errorf("to node already exists")
	}

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
// ReplaceNode(n Node) error
func (ag *ArangoGraph) ReplaceNode(ni handler.Node) error {
	// convert ni to Node
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// check if the node exists
	exists, err := ag.checkItemExists(n.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.ID)
		return fmt.Errorf("node does not exist")
	}

	// update the node
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, n.Collection)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}

	doc := n.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	_, err = col.ReplaceDocument(ctx, n.ID, doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to replace document: %v", err)
		return err
	}

	// update the node info in the bidirectional map nodeNameToIDMap
	ag.nodeNameToIDMap.Put(n.Name, n.ID)
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

	// check if the edge exists
	exists, err := ag.checkItemExists(e.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for edge: %v", err)
		return err
	}

	if !exists {
		ag.logger.Fatal().Msgf("Edge %s does not exist", e.ID)
		return fmt.Errorf("edge does not exist")
	}

	// update the edge
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

	doc := e.Data
	doc["_id"] = e.ID
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship
	doc["_from"] = e.From
	doc["_to"] = e.To

	_, err = col.ReplaceDocument(ctx, e.ID, doc)

	if err != nil {
		ag.logger.Fatal().Msgf("Failed to replace document: %v", err)
		return err
	}

	return nil
}






// UpdateNode(n Node) error
func (ag *ArangoGraph) UpdateNode(ni handler.Node) error {
	// convert ni to Node
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// check if the node exists
	exists, err := ag.checkItemExists(n.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.ID)
		return fmt.Errorf("node does not exist")
	}

	// update the node
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, n.Collection)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err

	}

	doc := n.Data
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	_, err = col.UpdateDocument(ctx, n.ID, doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}

	// update the node info in the bidirectional map nodeNameToIDMap
	ag.nodeNameToIDMap.Put(n.Name, n.ID)
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

	// check if the edge exists
	exists, err := ag.checkItemExists(e.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for edge: %v", err)
		return err
	}

	if !exists {
		ag.logger.Fatal().Msgf("Edge %s does not exist", e.ID)
		return fmt.Errorf("edge does not exist")
	}

	// update the edge
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

	doc := e.Data
	doc["_id"] = e.ID
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship
	doc["_from"] = e.From
	doc["_to"] = e.To

	_, err = col.UpdateDocument(ctx, e.ID, doc)

	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}

	return nil
}






// MergeNode(n Node) error
func (ag *ArangoGraph) MergeNode(ni handler.Node) error {
	// convert ni to Node
	var n Node
	switch v := ni.(type) {
	case *Node:
		n = *v
	default:
		return fmt.Errorf("invalid input")
	}

	// check if the node exists
	exists, err := ag.checkItemExists(n.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for node: %v", err)
		return err
	}
	if !exists {
		ag.logger.Fatal().Msgf("Node %s does not exist", n.ID)
		return fmt.Errorf("node does not exist")
	}

	// merge the node
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return err
	}

	col, err := db.Collection(ctx, n.Collection)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}

	// get the old data
	var oldNode Node
	_, err = col.ReadDocument(ctx, n.ID, &oldNode)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to read document: %v", err)
		return err
	}

	// merge the data
	for k, v := range n.Data {
		oldNode.Data[k] = v
	}

	// update the node
	doc := oldNode.Data
	
	doc["_id"] = n.ID
	doc["name"] = n.Name
	doc["collection"] = n.Collection

	_, err = col.UpdateDocument(ctx, n.ID, doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to update document: %v", err)
		return err
	}

	// update the node info in the bidirectional map nodeNameToIDMap
	ag.nodeNameToIDMap.Put(n.Name, n.ID)
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

	// check if the edge exists
	exists, err := ag.checkItemExists(e.ID)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to check for edge: %v", err)
		return err
	}

	if !exists {
		ag.logger.Fatal().Msgf("Edge %s does not exist", e.ID)
		return fmt.Errorf("edge does not exist")
	}

	// merge the edge
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

	// get the old data
	var oldEdge Edge
	_, err = col.ReadDocument(ctx, e.ID, &oldEdge)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to read document: %v", err)
		return err
	}

	// merge the data
	for k, v := range e.Data {
		oldEdge.Data[k] = v
	}

	// update the edge
	doc := oldEdge.Data
	doc["_id"] = e.ID
	doc["collection"] = e.Collection
	doc["relationship"] = e.Relationship
	doc["_from"] = e.From
	doc["_to"] = e.To

	_, err = col.UpdateDocument(ctx, e.ID, doc)

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
	col, err := ag.db.Collection(context.Background(), infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return err
	}

	// delete the document by _id
	_, err = col.RemoveDocument(context.Background(), infos[1])
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

	// convert id to string
	idStr, ok := id.(string)
	if !ok {
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
	col, err := ag.db.Collection(context.Background(), infos[0])
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open collection: %v", err)
		return nil, err
	}

	// get the document by _id
	var doc Node
	_, err = col.ReadDocument(context.Background(), infos[1], &doc)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to get document: %v", err)
		return nil, err
	}

	return doc, nil
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

	n, ok := node.(handler.Node)
	if !ok {
		ag.logger.Fatal().Msgf("Invalid node: %v", node)
		return nil, fmt.Errorf("invalid node")
	}
	return n, nil
}

// GetNodesByRegex(regex string) ([]Node, error)
func (ag *ArangoGraph) GetNodesByRegex(regex string) ([]handler.Node, error) {
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR node IN nodes
    FILTER REGEX_MATCHES(node.name, @regex, true)
    RETURN node
	`
	bindVars := map[string]interface{}{
		"regex": regex,
	}

	cursor, err := db.Query(ctx, query, bindVars)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to execute query: %v", err)
		return nil, err
	}
	defer cursor.Close()

	var nodes []handler.Node
	for {
		var node handler.Node
		_, err := cursor.ReadDocument(ctx, &node)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			ag.logger.Fatal().Msgf("Failed to read document: %v", err)
			return nil, err
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetEdgesByRegex(regex string) ([]Edge, error)
func (ag *ArangoGraph) GetEdgesByRegex(regex string) ([]handler.Edge, error) {
	ctx := context.Background()
	db, err := ag.Client.Database(ctx, ag.dbname)
	if err != nil {
		ag.logger.Fatal().Msgf("Failed to open database: %v", err)
		return nil, err
	}

	query := `
	FOR edge IN edges
	FILTER REGEX_MATCHES(edge.relationship, @regex, true)
	RETURN edge
	`
	bindVars := map[string]interface{}{
		"regex": regex,
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
		node, err := ag.GetItemByID(edge.From)
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
		"id": id,
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