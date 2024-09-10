package local

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// ~ 01 Fundamental Function Section
// ReadJSONFile reads a JSON file and returns its contents as a map
func ReadJSONFile(filePath string) (map[string]interface{}, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func WriteJSONFile(filePath string, data interface{}) error {
	// check the filePath dir is exist. if not, create dir
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	// Open the file for writing, create it if it doesn't exist
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Encode the data to JSON and write it to the file
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Optional: for pretty-printing
	if err := encoder.Encode(data); err != nil {
		return err
	}

	return nil
}

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

// // NodeWithLevel 表示一个节点及其在 BFS 中的层次
// type NodeWithLevel struct {
// 	Node  *Node // 图中的节点
// 	Level int   // 节点所在的层次
// }

// // BFSWithLevelsStruct 实现广度优先搜索并返回每个节点及其层次
// func (db *InMemoryDB) BFSWithLevelsStruct(startID string) []NodeWithLevel {
// 	db.m.RLock()
// 	defer db.m.RUnlock()

// 	startNode, exists := db.Nodes[startID]
// 	if !exists {
// 		return nil
// 	}

// 	visited := make(map[string]bool)
// 	queue := []NodeWithLevel{{Node: startNode, Level: 0}}
// 	var result []NodeWithLevel

// 	for len(queue) > 0 {
// 		nodeWithLevel := queue[0]
// 		queue = queue[1:]

// 		if !visited[nodeWithLevel.Node.ID] {
// 			result = append(result, nodeWithLevel)
// 			visited[nodeWithLevel.Node.ID] = true

// 			for _, edge := range db.Edges {
// 				if edge.From.ID == nodeWithLevel.Node.ID && !visited[edge.To.ID] {
// 					queue = append(queue, NodeWithLevel{Node: edge.To, Level: nodeWithLevel.Level + 1})
// 				}
// 			}
// 		}
// 	}

// 	return result
// }

// // NodeWithPath 表示一个节点及其从起始节点到该节点的路径
// type NodeWithPath struct {
// 	Node *Node    // 当前节点
// 	Path []string // 从起始节点到该节点的路径
// }

// // BFSWithPaths 实现广度优先搜索并返回每个节点及其路径
// func (db *InMemoryDB) BFSWithPaths(startID string) []NodeWithPath {
// 	db.m.RLock()
// 	defer db.m.RUnlock()

// 	startNode, exists := db.Nodes[startID]
// 	if !exists {
// 		return nil
// 	}

// 	visited := make(map[string]bool)
// 	queue := []NodeWithPath{{Node: startNode, Path: []string{startID}}}
// 	var result []NodeWithPath

// 	for len(queue) > 0 {
// 		nodeWithPath := queue[0]
// 		queue = queue[1:]

// 		if !visited[nodeWithPath.Node.ID] {
// 			result = append(result, nodeWithPath)
// 			visited[nodeWithPath.Node.ID] = true

// 			for _, edge := range db.Edges {
// 				if edge.From.ID == nodeWithPath.Node.ID && !visited[edge.To.ID] {
// 					newPath := append([]string{}, nodeWithPath.Path...)
// 					newPath = append(newPath, edge.To.ID)
// 					queue = append(queue, NodeWithPath{Node: edge.To, Path: newPath})
// 				}
// 			}
// 		}
// 	}

// 	return result
// }

// // DFSResult represents the result of a DFS traversal
// type DFSResult struct {
// 	NodeID string
// 	Path   []string
// 	Level  int
// }

// // DFS 实现深度优先搜索
// func (db *InMemoryDB) DFS(startID string) []DFSResult {
// 	db.m.RLock()
// 	defer db.m.RUnlock()

// 	startNode, exists := db.Nodes[startID]
// 	if !exists {
// 		return nil
// 	}

// 	var result []DFSResult
// 	visited := make(map[string]bool)
// 	var path []string

// 	var dfs func(node *Node, level int)
// 	dfs = func(node *Node, level int) {
// 		if visited[node.ID] {
// 			return
// 		}
// 		visited[node.ID] = true
// 		path = append(path, node.ID)

// 		// Create a copy of the current path to store in the result
// 		pathCopy := make([]string, len(path))
// 		copy(pathCopy, path)
// 		result = append(result, DFSResult{NodeID: node.ID, Path: pathCopy, Level: level})

// 		for _, edge := range db.Edges {
// 			if edge.From.ID == node.ID && !visited[edge.To.ID] {
// 				dfs(edge.To, level+1)
// 			}
// 		}

// 		// Backtrack
// 		path = path[:len(path)-1]
// 	}

// 	dfs(startNode, 0)
// 	return result
// }

// // DFSWithLevelsStruct 实现深度优先搜索并返回每个节点及其层次
// func (db *InMemoryDB) DFSWithLevelsStruct(startID string) []NodeWithLevel {
// 	db.m.RLock()
// 	defer db.m.RUnlock()

// 	startNode, exists := db.Nodes[startID]
// 	if !exists {
// 		return nil
// 	}

// 	var result []NodeWithLevel
// 	visited := make(map[string]bool)

// 	var dfs func(node *Node, level int)
// 	dfs = func(node *Node, level int) {
// 		if visited[node.ID] {
// 			return
// 		}
// 		visited[node.ID] = true
// 		result = append(result, NodeWithLevel{Node: node, Level: level})

// 		for _, edge := range db.Edges {
// 			if edge.From.ID == node.ID && !visited[edge.To.ID] {
// 				dfs(edge.To, level+1)
// 			}
// 		}
// 	}

// 	dfs(startNode, 0)
// 	return result
// }

// // DFSWithPaths 实现深度优先搜索并返回每个节点及其路径
// func (db *InMemoryDB) DFSWithPaths(startID string) []NodeWithPath {
// 	db.m.RLock()
// 	defer db.m.RUnlock()

// 	startNode, exists := db.Nodes[startID]
// 	if !exists {
// 		return nil
// 	}

// 	var result []NodeWithPath
// 	visited := make(map[string]bool)

// 	var dfs func(node *Node, path []string)
// 	dfs = func(node *Node, path []string) {
// 		if visited[node.ID] {
// 			return
// 		}
// 		visited[node.ID] = true
// 		newPath := append(path, node.ID)
// 		result = append(result, NodeWithPath{Node: node, Path: newPath})

// 		for _, edge := range db.Edges {
// 			if edge.From.ID == node.ID && !visited[edge.To.ID] {
// 				dfs(edge.To, newPath)
// 			}
// 		}
// 	}

// 	dfs(startNode, []string{})
// 	return result
// }

// DFSWithCompletePaths performs a depth-first search starting from the specified node ID and returns a list of complete paths.
// A complete path is a sequence of node IDs from the starting node to a leaf node, where a leaf node is a node with no unvisited children.
// If the starting node does not exist in the database, it returns nil.
//
// Parameters:
// - startID: The ID of the starting node for the depth-first search.
//
// Returns:
// - [][]string: A list of complete paths, where each path is represented as a slice of node IDs.
func (db *InMemoryDB) DFSWithCompletePaths(startID string) [][]string {
	db.m.RLock()
	defer db.m.RUnlock()

	startNode, exists := db.Nodes[startID]
	if !exists {
		return nil
	}

	var result [][]string
	visited := make(map[string]bool)

	var dfs func(node *Node, path []string)
	dfs = func(node *Node, path []string) {
		visited[node.ID] = true
		newPath := append(path, node.ID)

		// Check if this is a leaf node (no unvisited children)
		isLeaf := true
		for _, edge := range db.Edges {
			if edge.From.ID == node.ID && !visited[edge.To.ID] {
				isLeaf = false
				dfs(edge.To, newPath)
			}
		}

		// If it's a leaf node, add the path to the result
		if isLeaf {
			result = append(result, newPath)
		}

		// Mark this node as unvisited to allow other paths to visit it
		visited[node.ID] = false
	}

	dfs(startNode, []string{})
	return result
}

// ~ 01 Fundamental Function Section End

// ~ 02 Handler Interface Inplemetation Section
// Init 用于初始化图数据库
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

// Connect 用于连接图数据库
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

	// iterate over the directories
	for _, dir := range dirs {
		// check if the directory is a directory
		if !dir.IsDir() {
			// InMemoryDB.json file is just for other query
			// the node and edge files are dominant in creating the graph
		} else {
			// get the name of the directory
			collection := dir.Name()
			// get all the files in the directory
			files, err := os.ReadDir(filepath.Join(db.configPath, collection))
			if err != nil {
				return err
			}

			// iterate over the files
			for _, file := range files {
				// get the name of the file
				fileName := file.Name()
				// get the path of the file
				filePath := filepath.Join(db.configPath, collection, fileName)

				// read the file
				data, err := ReadJSONFile(filePath)
				if err != nil {
					return err
				}

				// check if the file is a node or an edge
				if _, ok := data["From"]; ok {
					// create an edge
					from, to := data["From"].(string), data["To"].(string)
					tmpWeight, ok := data["Weight"].(float64)
					if !ok {
						tmpWeight, _ = strconv.ParseFloat(data["Weight"].(string), 64)
					}

					edge, err := NewEdge(
						WithEID(data["ID"].(string)),
						WithEName(data["Name"].(string)),
						WithECollection(collection), // should be the same as the data["Collection"] field
						WithEFrom(db.Nodes[from]),
						WithETo(db.Nodes[to]),
						WithEWeight(int(tmpWeight)),
						WithEData(data))

					if err != nil {
						return err
					}

					db.Edges[edge.ID] = edge
					// add the edge id and name to the bidimap
					db.EdgeNameMap.Put(edge.ID, edge.Name)
				} else {
					// create a node
					node, err := NewNode(
						WithNID(data["ID"].(string)),
						WithNName(data["Name"].(string)),
						WithNCollection(collection),
						WithNData(data))
					if err != nil {
						return err
					}

					db.Nodes[node.ID] = node
					// add the node id and name to the bidimap
					db.NodeNameMap.Put(node.ID, node.Name)
				}
			}
		}
	}

	return nil
}

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
		err := WriteJSONFile(filepath.Join(db.configPath, edge.Collection, edge.Name+".json"), edge.Export())
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

func (db *InMemoryDB) AddNode(n *Node) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[n.ID]; ok {
		return fmt.Errorf("node with ID %s already exists", n.ID)
	}

	db.Nodes[n.ID] = n
	return nil
}

func (db *InMemoryDB) AddVertex(collection string, data map[string]interface{}) (id string, err error) {
	db.m.Lock()
	defer db.m.Unlock()
	defer recoverFromPanic(&err)

	n, err := NewNode(WithNCollection(collection), WithNData(data))
	if err != nil {
		return "", err
	}

	// check if the ID is already in the Nodes
	// if yes, merge the data
	// if no , add the node to the Nodes
	if _, ok := db.Nodes[n.ID]; !ok {
		db.Nodes[n.ID] = n
	} else {
		db.Nodes[n.ID].Collection = n.Collection
		db.Nodes[n.ID].Data = MergeMaps(db.Nodes[n.ID].Data, n.Data)

	}

	return n.ID, err
}

func (db *InMemoryDB) AddLink(e *Edge) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Edges[e.ID]; ok {
		return fmt.Errorf("edge with ID %s already exists", e.ID)
	}

	db.Edges[e.ID] = e
	return nil
}

func (db *InMemoryDB) AddEdge(collection string, from, to string, data map[string]interface{}) (id string, err error) {
	db.m.Lock()
	defer db.m.Unlock()
	defer recoverFromPanic(&err)

	// ~ check if the from and to nodes exist
	if _, ok := db.Nodes[from]; !ok {
		return "", fmt.Errorf("node with ID %s does not exist", from)
	}
	if _, ok := db.Nodes[to]; !ok {
		return "", fmt.Errorf("node with ID %s does not exist", to)
	}

	e, err := NewEdge(
		WithECollection(collection),

		WithEFrom(db.Nodes[from]),
		WithETo(db.Nodes[to]),
		WithEData(data))
	if err != nil {
		return "", err
	}

	db.Edges[e.ID] = e

	return e.ID, err
}

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


func (db *InMemoryDB) GetVertexDB(id string) (map[string]interface{}, error) {
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

func (db *InMemoryDB) GetFromVertices(id string) ([]string, error) {
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

func (db *InMemoryDB) GetToVertices(id string) ([]string, error) {
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

func (db *InMemoryDB) UpdateVertex(id string, data map[string]interface{}) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[id]; !ok {
		return fmt.Errorf("node with ID %s does not exist", id)
	}
	db.Nodes[id].Data = MergeMaps(db.Nodes[id].Data, data)
	return nil
}

func (db *InMemoryDB) UpdateEdge(id, from, to string, data map[string]interface{}) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[from]; !ok {
		return fmt.Errorf("node with ID %s does not exist", from)
	}
	if _, ok := db.Nodes[to]; !ok {
		return fmt.Errorf("node with ID %s does not exist", to)
	}

	if _, ok := db.Edges[id]; !ok {
		return fmt.Errorf("edge with ID %s does not exist", id)
	}

	db.Edges[id].From = db.Nodes[from]
	db.Edges[id].To = db.Nodes[to]
	db.Edges[id].Data = MergeMaps(db.Edges[id].Data, data)
	return nil
}

func (db *InMemoryDB) DeleteVertex(id string) error {
	db.m.Lock()
	defer db.m.Unlock()

	if _, ok := db.Nodes[id]; !ok {
		return fmt.Errorf("node with ID %s does not exist", id)
	}

	delete(db.Nodes, id)

	for _, e := range db.Edges {
		if e.From.ID == id || e.To.ID == id {
			delete(db.Edges, e.ID)
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
