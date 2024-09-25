package local

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/wonderstone/chainstorm/handler"
	"gopkg.in/yaml.v3"
)

// implement the GraphDB interface

// - Init operations
// Init(yamlPath string) error
func (db *InMemoryDB) Init(yamlPath string) error {
	// read the config yaml file from the yamlPath
	// parse the yaml file and set the
	// db.configPath is equal to dataPath field in the yaml file
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

	// set the db.configPath
	dataPath, ok := data["dataPath"].(string)
	if !ok {
		return fmt.Errorf("dataPath is not a string")
	}
	db.configPath = dataPath
	return nil
}

// - Connection operations

// checkType function is used to check if the data is a node or an edge or neither
func checkType(data map[string]interface{}) string {
	// if data has all the mandatory fields of a node, return "node"
	if _, ok := data["ID"]; ok {
		if _, ok := data["Name"]; ok {
			if _, ok := data["Collection"]; ok {
				return "node"
			}
		}
	}
	// if data has all the mandatory fields of an edge, return "edge"
	if _, ok := data["From"]; ok {
		if _, ok := data["To"]; ok {
			if _, ok := data["Collection"]; ok {
				if _, ok := data["ID"]; ok {
					if _, ok := data["Relationship"]; ok {
						return "edge"
					}
				}
			}
		}
	}
	// if data is neither a node nor an edge, return "neither"
	return "neither"
}

// Connect() error
// 读取本地文件并将其内容加载到内存中
func (db *InMemoryDB) Connect() error {
	// read the configPath file
	// every collection is a directory in the configPath
	// get all the json files in each collection
	// read the json files and check if it is a node or an edge
	// add the node or edge to the db.Nodes or db.Edges

	// get all the directories in the configPath
	dirs, err := os.ReadDir(db.configPath)
	if err != nil {
		return err
	}

	// iterate over the directories the first time for nodes only
	for _, dir := range dirs {
		if dir.IsDir() {
			// get all the json files in the directory
			files, err := os.ReadDir(filepath.Join(db.configPath, dir.Name()))
			if err != nil {
				return err
			}

			// iterate over the files
			for _, file := range files {
				// read the json file
				data, err := ReadJSONFile(filepath.Join(db.configPath, dir.Name(), file.Name()))
				if err != nil {
					return err
				}
				// check if the data is a node
				if checkType(data) == "node" {

					// check if the node has the data field
					if _, ok := data["Data"]; !ok {
						// create a new node
						node, err := NewNode(
							WithNID(data["ID"].(string)),
							WithNName(data["Name"].(string)),
							WithNCollection(data["Collection"].(string)),
						)
						if err != nil {
							return err
						}
						// add the node to the db.Nodes
						db.Nodes[node.ID] = node
						// add the node name to the nodeNameSet
						db.nodeNameSet[node.Name] = void{}
						// add the node name and id to the NodeNameMap
						db.NodeNameMap.Put(node.Name, node.ID)

					} else {
						// create a new node with the data field
						node, err := NewNode(
							WithNID(data["ID"].(string)),
							WithNName(data["Name"].(string)),
							WithNCollection(data["Collection"].(string)),
							WithNData(data["Data"].(map[string]interface{})),
						)
						if err != nil {
							return err
						}
						// add the node to the db.Nodes
						db.Nodes[node.ID] = node
						// add the node name to the nodeNameSet
						db.nodeNameSet[node.Name] = void{}
						// add the node name and id to the NodeNameMap
						db.NodeNameMap.Put(node.Name, node.ID)
					}
				}
			}
		}
	}

	// iterate over the directories the second time for edges only
	for _, dir := range dirs {
		if dir.IsDir() {
			// get all the json files in the directory
			files, err := os.ReadDir(filepath.Join(db.configPath, dir.Name()))
			if err != nil {
				return err
			}
			// iterate over the files
			for _, file := range files {
				// read the json file
				data, err := ReadJSONFile(filepath.Join(db.configPath, dir.Name(), file.Name()))
				if err != nil {
					return err
				}

				// check if the from node and to node exist with checkNodeNameExists method
				if !db.checkNodeNameExists(data["From"].(string)) {
					return fmt.Errorf("node with name %s does not exist", data["From"].(string))
				}

				if !db.checkNodeNameExists(data["To"].(string)) {
					return fmt.Errorf("node with name %s does not exist", data["To"].(string))
				}
				// check if the data is an edge
				if checkType(data) == "edge" {
					// check if the edge has the data field
					if _, ok := data["Data"]; !ok {
						// create a new edge
						edge, err := NewEdge(
							WithEID(data["ID"].(string)),
							WithEName(data["Relationship"].(string)),
							WithECollection(data["Collection"].(string)),
							WithEFrom(db.Nodes[data["From"].(string)]),
							WithETo(db.Nodes[data["To"].(string)]),
						)
						if err != nil {
							return err
						}
						// add the edge to the db.Edges
						db.Edges[edge.ID] = edge
					} else {
						// create a new edge with the data field
						edge, err := NewEdge(
							WithEID(data["ID"].(string)),
							WithEName(data["Relationship"].(string)),
							WithECollection(data["Collection"].(string)),
							WithEFrom(db.Nodes[data["From"].(string)]),
							WithETo(db.Nodes[data["To"].(string)]),
							WithEData(data["Data"].(map[string]interface{})),
						)
						if err != nil {
							return err
						}
						// add the edge to the db.Edges
						db.Edges[edge.ID] = edge
					}
				}
			}
		}
	}

	return nil
}

// Disconnect() error
// 将内存中的数据写入到本地文件
func (db *InMemoryDB) Disconnect() error {
	// iterate over the db.Nodes
	// write the node to the file
	for _, node := range db.Nodes {
		err := WriteJSONFile(filepath.Join(db.configPath, node.Collection, node.Name+".json"), node.Export())
		if err != nil {
			return err
		}
	}

	// iterate over the db.Edges
	// write the edge to the file
	for _, edge := range db.Edges {
		err := WriteJSONFile(filepath.Join(db.configPath, edge.Collection, edge.Relationship+".json"), edge.Export())
		if err != nil {
			return err
		}
	}

	return nil
}
// - CRUD operations
// + Create operations
// AddNode(n Node) (interface{}, error)

func (db *InMemoryDB) AddNode(ni handler.Node) (interface{}, error) {
	db.m.Lock()
	defer db.m.Unlock()
	// check ni type
	// if ni is a pointer, use ni.(*Node)
	// if ni is a value, use ni.(Node)
	var n Node
	switch v := ni.(type) {
	// case Node: // removed as Node cannot have dynamic type Node
	// 	n = v
	case *Node:
		n = *v
	default:
		return nil, fmt.Errorf("invalid input")
	}


	// check if node has mandatory fields
	if n.ID == "" {
		// give uuid
		n.ID = uuid.New().String()
	} else {
		// check if the ID is already in the Nodes
		if _, ok := db.Nodes[n.ID]; ok {
			return nil, fmt.Errorf("node with ID %s already exists", n.ID)
		}
	}
	if n.Name == "" {
		return nil, fmt.Errorf("node name is required")
	} else {
		// check if the name is already in the nodeNameSet by checkNodeNameExists
		if db.checkNodeNameExists(n.Name) {
			return nil, fmt.Errorf("node with name %s already exists", n.GetName())
		}
	}
	if n.Collection == "" {
		return nil, fmt.Errorf("node collection is required")
	}

	// add the node to the Nodes and NodeNameMap
	db.Nodes[n.ID] = &n
	// add the node id and name to the bidimap
	db.NodeNameMap.Put(n.Name, n.ID)
	// add the nodename to the nodeNameSet
	db.nodeNameSet[n.Name] = void{}
	return n.ID, nil
}

// AddEdge(e Edge) (interface{}, error)
func (db *InMemoryDB) AddEdge(ei handler.Edge) (interface{}, error) {
	db.m.Lock()
	defer db.m.Unlock()
	// check ei type
	// if ei is a pointer, use ei.(*Edge)
	// if ei is a value, use ei.(Edge)
	var e Edge
	switch v := ei.(type) {
	// case Edge: // removed as Edge cannot have dynamic type Edge
	// 	e = v
	case *Edge:
		e = *v
	default:
		return nil, fmt.Errorf("invalid input")
	}

	// check if edge has mandatory fields
	if e.ID == "" {
		// give uuid
		e.ID = uuid.New().String()
	} else {
		// check if the ID is already in the Edges
		if _, ok := db.Edges[e.ID]; ok {
			return nil, fmt.Errorf("edge with ID %s already exists", e.ID)
		}
	}
	if e.Relationship == "" {
		return nil, fmt.Errorf("edge name is required")
	} 
	if e.Collection == "" {
		return nil, fmt.Errorf("edge collection is required")
	}
	if e.From == nil {
		return nil, fmt.Errorf("edge from node is required")
	} else{
		// check if the from node exists
		if _, ok := db.Nodes[e.From.ID]; !ok {
			return nil, fmt.Errorf("node with ID %s does not exist", e.From.ID)
		}
	}
	if e.To == nil {
		return nil, fmt.Errorf("edge to node is required")
	} else {
		// check if the to node exists
		if _, ok := db.Nodes[e.To.ID]; !ok {
			return nil, fmt.Errorf("node with ID %s does not exist", e.To.ID)
		}
	}

	// add the edge to the Edges and EdgeNameMap
	db.Edges[e.ID] = &e

	return e.ID, nil
}
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
// GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)

// ~ 01 Fundamental Function Section

// MergeMaps merges two maps and returns the result
func MergeMaps(map1, map2 map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})

	for k, v := range map1 {
		merged[k] = v
	}

	for k, v := range map2 {
		merged[k] = v
	}

	return merged
}

// recoverFromPanic recovers from a panic and sets the error if one occurs
func recoverFromPanic(err *error) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			*err = e
		} else {
			*err = fmt.Errorf("panic: %v", r)
		}
	}
}

// BFS 实现广度优先搜索
// BFSWithLevels 实现广度优先搜索并返回每一层的节点
func (db *InMemoryDB) BFSWithLevels(startID string) [][]string {
	db.m.RLock()
	defer db.m.RUnlock()

	startNode, exists := db.Nodes[startID]
	if !exists {
		return nil
	}

	visited := make(map[string]bool)
	queue := []*Node{startNode}
	var result [][]string

	for len(queue) > 0 {
		levelSize := len(queue)
		var currentLevel []string

		for i := 0; i < levelSize; i++ {
			node := queue[0]
			queue = queue[1:]

			if !visited[node.ID] {
				currentLevel = append(currentLevel, node.ID)
				visited[node.ID] = true

				for _, edge := range db.Edges {
					if edge.From.ID == node.ID && !visited[edge.To.ID] {
						queue = append(queue, edge.To)
					}
				}
			}
		}

		if len(currentLevel) > 0 {
			result = append(result, currentLevel)
		}
	}

	return result
}

// BFSWithWeightRange 实现广度优先搜索并返回路径权重在[min, max]之间的节点
func (db *InMemoryDB) BFSWithWeightRange(startID string, minWeight, maxWeight int) [][]string {
	db.m.RLock()
	defer db.m.RUnlock()

	startNode, exists := db.Nodes[startID]
	if !exists {
		return nil
	}

	visited := make(map[string]bool)
	queue := []struct {
		node        *Node
		totalWeight int
		path        []string
	}{{node: startNode, totalWeight: 0, path: []string{startNode.ID}}}

	var result [][]string

	for len(queue) > 0 {
		elem := queue[0]
		queue = queue[1:]
		node, weight, path := elem.node, elem.totalWeight, elem.path

		if visited[node.ID] {
			continue
		}
		visited[node.ID] = true

		// Check if the path weight is in the specified range
		if weight >= minWeight && weight <= maxWeight {
			result = append(result, path)
		}

		for _, edge := range db.Edges {
			if edge.From.ID == node.ID && !visited[edge.To.ID] {
				newWeight := weight + edge.Weight
				newPath := append([]string(nil), path...)
				newPath = append(newPath, edge.To.ID)
				queue = append(queue, struct {
					node        *Node
					totalWeight int
					path        []string
				}{node: edge.To, totalWeight: newWeight, path: newPath})
			}
		}
	}

	return result
}

// ~ 01 Fundamental Function Section End

// ~ 02 Handler Interface Inplemetation Section

// Connect 用于连接图数据库

// Disconnect 用于断开图数据库的连接
// 将内存中的数据写入到本地文件
func (db *InMemoryDB) Disconnect() error {
	// iterate over the db.Nodes
	// write the node to the file
	for _, node := range db.Nodes {
		err := WriteJSONFile(filepath.Join(db.configPath, node.Collection, node.Name+".json"), node.Export())
		if err != nil {
			return err
		}
	}

	// iterate over the db.Edges
	// write the edge to the file
	for _, edge := range db.Edges {
		err := WriteJSONFile(filepath.Join(db.configPath, edge.Collection, edge.Relationship+".json"), edge.Export())
		if err != nil {
			return err
		}
	}

	// output hte NodeNameMap and EdgeNameMap to the file
	err := WriteJSONFile(filepath.Join(db.configPath, "InMemoryDB.json"), db.Export())
	if err != nil {
		return err
	}

	return nil
}

// & AddFunc Section
func (db *InMemoryDB) AddNode(n *Node) error {
	db.m.Lock()
	defer db.m.Unlock()
	// check if node has mandatory fields
	if n.ID == "" {
		return fmt.Errorf("node ID is required")
	}
	if n.Name == "" {
		return fmt.Errorf("node name is required")
	}
	if n.Collection == "" {
		return fmt.Errorf("node collection is required")
	}
	// check if the ID is already in the Nodes
	if _, ok := db.Nodes[n.ID]; ok {
		return fmt.Errorf("node with ID %s already exists", n.ID)
	}
	// check if the name is already in the nodeNameSet by checkNodeNameExists
	if db.checkNodeNameExists(n.Name) {
		return fmt.Errorf("node with name %s already exists", n.Name)
	}
	// check if the name is already in the NodeNameMap
	if _, ok := db.NodeNameMap.Get(n.Name); ok {
		return fmt.Errorf("node with name %s already exists", n.Name)
	}
	// add the node to the Nodes and NodeNameMap
	db.Nodes[n.ID] = n
	// add the node id and name to the bidimap
	db.NodeNameMap.Put(n.Name, n.ID)
	// add the nodename to the nodeNameSet
	db.nodeNameSet[n.Name] = void{}

	return nil
}

func (db *InMemoryDB) AddEdge(e *Edge) error {
	db.m.Lock()
	defer db.m.Unlock()
	// check if edge has mandatory fields
	if e.ID == "" {
		return fmt.Errorf("edge ID is required")
	}
	if e.Relationship == "" {
		return fmt.Errorf("edge name is required")
	}
	if e.Collection == "" {
		return fmt.Errorf("edge collection is required")
	}
	if e.From == nil {
		return fmt.Errorf("edge from node is required")
	}
	if e.To == nil {
		return fmt.Errorf("edge to node is required")
	}
	// check if the ID is already in the Edges
	if _, ok := db.Edges[e.ID]; ok {
		return fmt.Errorf("edge with ID %s already exists", e.ID)
	}
	// check if the name is already in the edgeNameSet by checkEdgeNameExists
	if db.checkEdgeNameExists(e.Relationship) {
		return fmt.Errorf("edge with name %s already exists", e.Relationship)
	}
	// check if the name is already in the EdgeNameMap
	if _, ok := db.EdgeNameMap.Get(e.Relationship); ok {
		return fmt.Errorf("edge with name %s already exists", e.Relationship)
	}

	// add the edge to the Edges and EdgeNameMap
	db.Edges[e.ID] = e
	// add the edge id and name to the bidimap
	db.EdgeNameMap.Put(e.Relationship, e.ID)
	// add the edgeName to the edgeNameSet
	db.edgeNameSet[e.Relationship] = void{}

	return nil
}

// ~ Read Func Section

// get node id with the node name, regexp function is used
// regStr is the str to build regular expression
// targetStr is the string to be matched
type regexpFunc func(targetStr string, regStr string) bool

func (db *InMemoryDB) GetNodeIDs(SearchChars string, regfunc regexpFunc) ([]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	var result []string
	for _, k := range db.NodeNameMap.Values() {
		if regfunc(k.(string), SearchChars) {
			key, found := db.NodeNameMap.GetKey(k)
			if found {
				result = append(result, key.(string))
			}
		} else {
			continue
		}

	}

	return result, nil
}

func (db *InMemoryDB) GetNodeDB(id string) (map[string]interface{}, error) {
	db.m.RLock()
	defer db.m.RUnlock()
	// add ID and Collection to the return value

	if n, ok := db.Nodes[id]; ok {
		n.Data["ID"] = n.ID
		n.Data["Collection"] = n.Collection
		return n.Data, nil
	}
	return nil, fmt.Errorf("node with ID %s does not exist", id)
}

func (db *InMemoryDB) GetEdgeDB(id string) (map[string]interface{}, error) {
	db.m.RLock()
	defer db.m.RUnlock()
	// add ID , Collection , from , to  and weight to the return value
	if e, ok := db.Edges[id]; ok {
		e.Data["ID"] = e.ID
		e.Data["Collection"] = e.Collection
		e.Data["From"] = e.From.ID
		e.Data["To"] = e.To.ID
		e.Data["Weight"] = e.Weight
		return e.Data, nil
	}
	return nil, fmt.Errorf("edge with ID %s does not exist", id)
}

// Given a node ID, GetFromVertices returns a list of node IDs that have an edge pointing to the specified node.
func (db *InMemoryDB) GetFromNodes(id string) ([]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	var fromVertices []string
	for _, e := range db.Edges {
		if e.To.ID == id {
			fromVertices = append(fromVertices, e.From.ID)
		}
	}

	return fromVertices, nil
}

// Given a node ID, GetToVertices returns a list of node IDs that the specified node has an edge pointing to.
func (db *InMemoryDB) GetToNodes(id string) ([]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	var toVertices []string
	for _, e := range db.Edges {
		if e.From.ID == id {
			toVertices = append(toVertices, e.To.ID)
		}
	}

	return toVertices, nil
}

// Given a node ID, GetInEdges returns a list of edge IDs that point to the specified node.
func (db *InMemoryDB) GetInEdges(id string) ([]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	var inEdges []string
	for _, e := range db.Edges {
		if e.To.ID == id {
			inEdges = append(inEdges, e.ID)
		}
	}

	return inEdges, nil
}

// Given a node ID, GetOutEdges returns a list of edge IDs that start from the specified node.
func (db *InMemoryDB) GetOutEdges(id string) ([]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	var outEdges []string
	for _, e := range db.Edges {
		if e.From.ID == id {
			outEdges = append(outEdges, e.ID)
		}
	}

	return outEdges, nil
}

// ~ Update Func Section
// UpdateNodeData updates the data of a node with the specified ID.
// It merges the existing data with the new data and updates the node in the database.
func (db *InMemoryDB) UpdateNodeData(id string, data map[string]interface{}) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[id]; !ok {
		return fmt.Errorf("node with ID %s does not exist", id)
	}
	db.Nodes[id].Data = MergeMaps(db.Nodes[id].Data, data)
	return nil
}

// UpdateEdgeData updates the data of an edge with the specified ID.
// It merges the existing data with the new data and updates the edge in the database.
func (db *InMemoryDB) UpdateEdgeData(id string, data map[string]interface{}) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Edges[id]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", id)
	}
	db.Edges[id].Data = MergeMaps(db.Edges[id].Data, data)
	return nil
}

// UpdateNodeName updates the name of a node with the specified ID.
// node would be replaced by the new node
func (db *InMemoryDB) UpdateNode(n *Node) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[n.ID]; !ok {
		return fmt.Errorf("node with ID %s does not exist", n.ID)
	}

	db.Nodes[n.ID] = n
	return nil
}

// UpdateEdge updates the edge with the specified ID.
// edge would be replaced by the new edge
func (db *InMemoryDB) UpdateEdge(e *Edge) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Edges[e.ID]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", e.ID)
	}

	db.Edges[e.ID] = e
	return nil
}

// ~ Delete Func Section
func (db *InMemoryDB) DeleteNode(id string) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[id]; !ok {
		return fmt.Errorf("node with ID %s does not exist", id)
	}
	// delete the node from the Nodes and NodeNameMap
	delete(db.Nodes, id)
	// delete the node id and name from the bidimap
	db.NodeNameMap.Remove(id)

	for _, e := range db.Edges {
		if e.From.ID == id || e.To.ID == id {
			// delete the edge from the Edges and EdgeNameMap
			delete(db.Edges, e.ID)
			// delete the edge id and name from the bidimap
			db.EdgeNameMap.Remove(e.ID)

		}
	}

	return nil
}

func (db *InMemoryDB) DeleteEdge(id string) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Edges[id]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", id)
	}

	delete(db.Edges, id)
	return nil
}

// GetAllRelatedVertices retrieves all the related vertices of a given node ID.
// It performs a breadth-first search (BFS) starting from the specified node ID and returns the vertices in levels.
// The function acquires a read lock on the database before performing the search.
// If the node with the specified ID does not exist in the database, it returns an error.
// The returned value is a 2D slice where each inner slice represents a level of vertices.
// The function returns nil and an error if the node does not exist, otherwise it returns the vertices and nil error.
func (db *InMemoryDB) GetAllRelatedVertices(id string) ([][]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	return db.BFSWithLevels(id), nil
}

func (db *InMemoryDB) GetAllRelatedEdges(id string) ([][]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	var result [][]string
	for _, edge := range db.Edges {
		if edge.From.ID == id {
			result = append(result, []string{"To", edge.ID, edge.To.ID})
		}

		if edge.To.ID == id {
			result = append(result, []string{"From", edge.ID, edge.From.ID})
		}

	}

	return result, nil
}

func (db *InMemoryDB) GetAllRelatedVerticesInEdgeSlice(id string, edgeSlice ...string) ([][]string, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	var result [][]string
	tmpEdges := make(map[string]*Edge)
	// create new graph with the same nodes but only the edges in the edgeSlice
	// iterate over the edgeSlice
	for _, edgeID := range edgeSlice {
		if edge, ok := db.Edges[edgeID]; ok {
			tmpEdges[edgeID] = edge
		} else {
			return nil, fmt.Errorf("edge with ID %s does not exist", edgeID)
		}
	}

	// new graph
	tmpGraph := &InMemoryDB{
		Nodes: db.Nodes,
		Edges: tmpEdges,
	}

	result, err := tmpGraph.GetAllRelatedVertices(id)
	return result, err
}

func (db *InMemoryDB) GetAllRelatedVerticesInRange(id string, min, max int) ([][]string, error) {
	var err error
	recoverFromPanic(&err)
	db.m.RLock()
	defer db.m.RUnlock()

	if _, ok := db.Nodes[id]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", id)
	}

	return db.BFSWithWeightRange(id, min, max), err

}
