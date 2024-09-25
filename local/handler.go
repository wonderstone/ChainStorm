package local

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

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
			return nil, fmt.Errorf("node with name %s already exists", n.Name)
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
	} else {
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

// + Update operations
// ReplaceNode(n Node) error

func (db *InMemoryDB) ReplaceNode(ni handler.Node) error {
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
		return fmt.Errorf("invalid input")
	}

	// check if the node exists
	if _, ok := db.Nodes[n.ID]; !ok {
		return fmt.Errorf("node with ID %s does not exist", n.ID)
	}

	// replace the node
	db.Nodes[n.ID] = &n
	return nil
}

// ReplaceEdge(e Edge) error
func (db *InMemoryDB) ReplaceEdge(ei handler.Edge) error {
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
		return fmt.Errorf("invalid input")
	}

	// check if the edge exists
	if _, ok := db.Edges[e.ID]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", e.ID)
	}

	// replace the edge
	db.Edges[e.ID] = &e
	return nil
}

// UpdateNode(n Node) error

func (db *InMemoryDB) UpdateNode(ni handler.Node) error {
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
		return fmt.Errorf("invalid input")
	}

	// check if the node exists
	if _, ok := db.Nodes[n.ID]; !ok {
		return fmt.Errorf("node with ID %s does not exist", n.ID)
	}

	// update the node
	db.Nodes[n.ID].Data = MergeMaps(db.Nodes[n.ID].Data, n.Data)
	return nil
}

// UpdateEdge(e Edge) error
func (db *InMemoryDB) UpdateEdge(ei handler.Edge) error {
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
		return fmt.Errorf("invalid input")
	}

	// check if the edge exists
	if _, ok := db.Edges[e.ID]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", e.ID)
	}

	// update the edge
	db.Edges[e.ID].Data = MergeMaps(db.Edges[e.ID].Data, e.Data)
	return nil
}

// MergeNode(n Node) error

func (db *InMemoryDB) MergeNode(ni handler.Node) error {
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
		return fmt.Errorf("invalid input")
	}

	// check if the node exists
	if _, ok := db.Nodes[n.ID]; !ok {
		return fmt.Errorf("node with ID %s does not exist", n.ID)
	}

	// merge the node
	db.Nodes[n.ID].Data = MergeMaps(db.Nodes[n.ID].Data, n.Data)
	return nil
}

// MergeEdge(e Edge) error
func (db *InMemoryDB) MergeEdge(ei handler.Edge) error {
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
		return fmt.Errorf("invalid input")
	}

	// check if the edge exists
	if _, ok := db.Edges[e.ID]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", e.ID)
	}

	// merge the edge
	db.Edges[e.ID].Data = MergeMaps(db.Edges[e.ID].Data, e.Data)
	return nil
}

// + Delete operations
// DeleteNode(name interface{}) error
func (db *InMemoryDB) DeleteNode(name interface{}) error {
	db.m.Lock()
	defer db.m.Unlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	// delete the node from the Nodes
	delete(db.Nodes, id.(string))
	// delete the node name from the nodeNameSet
	delete(db.nodeNameSet, name.(string))
	// delete the node name from the NodeNameMap
	db.NodeNameMap.Remove(name.(string))

	return nil
}

// DeleteItemByID(id interface{}) error
func (db *InMemoryDB) DeleteItemByID(id interface{}) error {
	db.m.Lock()
	defer db.m.Unlock()

	// check if the id exists
	if _, ok := db.Nodes[id.(string)]; ok {
		// delete the node from the Nodes
		delete(db.Nodes, id.(string))
		return nil
	}

	if _, ok := db.Edges[id.(string)]; ok {
		// delete the edge from the Edges
		delete(db.Edges, id.(string))
		return nil
	}

	return fmt.Errorf("item with ID %s does not exist", id)
}

// // + Query operations
// GetItemByID(id interface{}) (interface{}, error)
func (db *InMemoryDB) GetItemByID(id interface{}) (interface{}, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the id exists
	if node, ok := db.Nodes[id.(string)]; ok {
		return node, nil
	}

	if edge, ok := db.Edges[id.(string)]; ok {
		return edge, nil
	}

	return nil, fmt.Errorf("item with ID %s does not exist", id)
}

// GetNode(name interface{}) (Node, error)
func (db *InMemoryDB) GetNode(name interface{}) (handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	return db.Nodes[id.(string)], nil
}

// GetNodesByRegex(regex string) ([]Node, error)
func (db *InMemoryDB) GetNodesByRegex(regex string) ([]handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	var result []handler.Node
	for _, k := range db.NodeNameMap.Values() {
		if match, _ := regexp.MatchString(regex, k.(string)); match {
			key, found := db.NodeNameMap.GetKey(k)
			if found {
				result = append(result, db.Nodes[key.(string)])
			}
		}
	}

	return result, nil
}

// GetEdgesByRegex(regex string) ([]Edge, error)
func (db *InMemoryDB) GetEdgesByRegex(regex string) ([]handler.Edge, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	var result []handler.Edge
	for _, edge := range db.Edges {
		if match, _ := regexp.MatchString(regex, edge.Relationship); match {
			result = append(result, edge)
		}
	}

	return result, nil
}

// GetFromNodes(name interface{}) ([]Node, error)
func (db *InMemoryDB) GetFromNodes(name interface{}) ([]handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	var result []handler.Node
	for _, edge := range db.Edges {
		if edge.From.ID == id {
			result = append(result, edge.To)
		}
	}

	return result, nil
}

// GetToNodes(name interface{}) ([]Node, error)
func (db *InMemoryDB) GetToNodes(name interface{}) ([]handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	var result []handler.Node
	for _, edge := range db.Edges {
		if edge.To.ID == id {
			result = append(result, edge.From)
		}
	}

	return result, nil
}

// GetInEdges(name interface{}) ([]Edge, error)
func (db *InMemoryDB) GetInEdges(name interface{}) ([]handler.Edge, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	var result []handler.Edge
	for _, edge := range db.Edges {
		if edge.To.ID == id {
			result = append(result, edge)
		}
	}

	return result, nil
}

// GetOutEdges(name interface{}) ([]Edge, error)
func (db *InMemoryDB) GetOutEdges(name interface{}) ([]handler.Edge, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	var result []handler.Edge
	for _, edge := range db.Edges {
		if edge.From.ID == id {
			result = append(result, edge)
		}
	}

	return result, nil
}

// + Graph operations
// - Traversal operations
// GetAllRelatedNodes(name interface{}) ([][]Node, error)
func (db *InMemoryDB) GetAllRelatedNodes(name interface{}) ([][]handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// use BFSWithLevels to get all the related nodes
	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	return db.BFSWithLevels(id.(string))
}

// GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...Edge) ([][]Node, error)
func (db *InMemoryDB) GetAllRelatedNodesInEdgeSlice(name interface{}, edgeSlice ... handler.Edge) ([][]handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}



	// create a new graph
	newGraph, err := NewInMemoryDB()
	if err != nil {
		return nil, err
	}

	// the node would be the same as the original node
	newGraph.Nodes = db.Nodes

	// add the related edges to the newGraph
	for _, edge := range edgeSlice {
		// convert the edge to *Edge

		e := edge.(*Edge)

		newGraph.Edges[e.ID] = e
	}

	// update the 	configPath 	nodeNameSet map[string]void and	NodeNameMap *hashbidimap.Map

	newGraph.configPath = db.configPath
	newGraph.nodeNameSet = db.nodeNameSet
	newGraph.NodeNameMap = db.NodeNameMap

	// use GetAllRelatedNodes to get all the related nodes
	return newGraph.GetAllRelatedNodes(name)


}

// GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)

func (db *InMemoryDB) GetAllRelatedNodesInRange(name interface{}, max int) ([][]handler.Node, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	// check if the node name exists
	if !db.checkNodeNameExists(name.(string)) {
		return nil, fmt.Errorf("node with name %s does not exist", name)
	}

	// get the node id
	id, _ := db.NodeNameMap.Get(name.(string))

	// create a queue and a visited map
	queue := []string{id.(string)}
	visited := make(map[string]bool)
	visited[id.(string)] = true

	// create a result slice
	var result [][]handler.Node

	// iterate over the queue
	for len(queue) > 0 {
		var level []handler.Node
		// get the length of the queue
		l := len(queue)
		// iterate over the length
		for i := 0; i < l; i++ {
			// get the node from the queue
			node := db.Nodes[queue[0]]
			// remove the node from the queue
			queue = queue[1:]
			// add the node to the level
			level = append(level, node)
			// get the related nodes
			for _, edge := range db.Edges {
				if edge.From.ID == node.ID {
					if _, ok := visited[edge.To.ID]; !ok {
						queue = append(queue, edge.To.ID)
						visited[edge.To.ID] = true
					}
				}
			}
		}
		// add the level to the result
		result = append(result, level)
		// check if the max level is reached
		if len(result) == max {
			break
		}
	}

	return result, nil
}

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


// BFS 实现广度优先搜索
// BFSWithLevels 实现广度优先搜索并返回每一层的节点
func (db *InMemoryDB) BFSWithLevels(startID string) ([][]handler.Node, error) {
	// check if the startID exists
	if _, ok := db.Nodes[startID]; !ok {
		return nil, fmt.Errorf("node with ID %s does not exist", startID)
	}

	// create a queue and a visited map
	queue := []string{startID}
	visited := make(map[string]bool)
	visited[startID] = true

	// create a result slice
	var result [][]handler.Node

	// iterate over the queue
	for len(queue) > 0 {
		var level []handler.Node
		// get the length of the queue
		l := len(queue)
		// iterate over the length
		for i := 0; i < l; i++ {
			// get the node from the queue
			node := db.Nodes[queue[0]]
			// remove the node from the queue
			queue = queue[1:]
			// add the node to the level
			level = append(level, node)
			// get the related nodes
			for _, edge := range db.Edges {
				if edge.From.ID == node.ID {
					if _, ok := visited[edge.To.ID]; !ok {
						queue = append(queue, edge.To.ID)
						visited[edge.To.ID] = true
					}
				}
			}
		}
		// add the level to the result
		result = append(result, level)
	}

	return result, nil
}

// ~ 01 Fundamental Function Section End

// ~ 02 Handler Interface Inplemetation Section

// ~ Read Func Section
