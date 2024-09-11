package local

import (
	"fmt"
	"sync"

	"github.com/emirpasic/gods/maps/hashbidimap"
	"github.com/wonderstone/chainstorm/base"
)

type void struct{}

// InMemoryDB 代表整个图结构
type InMemoryDB struct {
	Nodes map[string]*base.Node // 节点集合, key is the ID of the node
	Edges map[string]*base.Edge // 边集合, key is the ID of the edge

	m           sync.RWMutex    // 用于并发控制的读写锁
	configPath  string          // 配置文件路径
	nodeNameSet map[string]void // 用于存储节点名称的集合 要建立node name的唯一性约束
	edgeNameSet map[string]void // 用于存储边名称的集合 要建立edge name的唯一性约束

	// BidiMap for ID : NodeName and ID : EdgeName
	NodeNameMap *hashbidimap.Map
	EdgeNameMap *hashbidimap.Map
}

func NewInMemoryDB() (*InMemoryDB, error) {
	db := &InMemoryDB{
		Nodes: make(map[string]*base.Node),
		Edges: make(map[string]*base.Edge),

		nodeNameSet: make(map[string]void),
		edgeNameSet: make(map[string]void),

		NodeNameMap: hashbidimap.New(),
		EdgeNameMap: hashbidimap.New(),
	}

	// Check if any of the initializations failed
	if db.Nodes == nil || db.Edges == nil || db.nodeNameSet == nil || db.edgeNameSet == nil || db.NodeNameMap == nil || db.EdgeNameMap == nil {
		return nil, fmt.Errorf("failed to initialize InMemoryDB")
	}

	return db, nil
}

// Export 用于导出NodeNameMap 和 EdgeNameMap 的数据 in json format
func (db *InMemoryDB) Export() map[string]interface{} {
	tmp := make(map[string]interface{})
	// iter all the nodes in the NodeNameMap and output the key and data pair in json
	nodeNameMap := make(map[string]string)
	for _, k := range db.NodeNameMap.Keys() {
		v, _ := db.NodeNameMap.Get(k)
		nodeNameMap[k.(string)] = v.(string)
	}
	tmp["NodeNameMap"] = nodeNameMap

	// iter all the edges in the EdgeNameMap and output the key and data pair in json
	edgeNameMap := make(map[string]string)
	for _, k := range db.EdgeNameMap.Keys() {
		v, _ := db.EdgeNameMap.Get(k)
		edgeNameMap[k.(string)] = v.(string)
	}

	tmp["EdgeNameMap"] = edgeNameMap

	return tmp
}

// Import 用于导入NodeNameMap 和 EdgeNameMap 的数据 in json format
func (db *InMemoryDB) Import(data map[string]interface{}) error {

	// check if the data contains the NodeNameMap key
	if _, ok := data["NodeNameMap"]; !ok {
		return fmt.Errorf("NodeNameMap is missing")
	}

	// check if the data contains the EdgeNameMap key
	if _, ok := data["EdgeNameMap"]; !ok {
		return fmt.Errorf("EdgeNameMap is missing")
	}

	// import the NodeNameMap data
	nodeNameMap, ok := data["NodeNameMap"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("NodeNameMap is not a map")
	}

	for k, v := range nodeNameMap {
		db.NodeNameMap.Put(k, v)
	}

	// import the EdgeNameMap data
	edgeNameMap, ok := data["EdgeNameMap"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("EdgeNameMap is not a map")
	}
	for k, v := range edgeNameMap {
		db.EdgeNameMap.Put(k, v)
	}

	return nil

}

// RegenerateSet iterates through all nodes and edges in the database,
// updating the nodeNameSet and edgeNameSet to ensure that each node
// and edge name is unique. If a duplicate name is found, an error is returned.
func (db *InMemoryDB) RegenerateSet() error {
	db.nodeNameSet = make(map[string]void)
	db.edgeNameSet = make(map[string]void)
	// Iterate over all nodes in the database to update the node name set.
	// This ensures that each node name is unique by checking for duplicates.
	for _, v := range db.Nodes {
		// Check if the node name is already in the set.
		// This ensures that each node name is unique within the graph.
		// If the node name is found in the set, return an error indicating the name is not unique.
		if _, ok := db.nodeNameSet[v.Name]; ok {
			return fmt.Errorf("node name %s is not unique", v.Name)
		}
		db.nodeNameSet[v.Name] = void{}
	}
	// Check if the edge name is already in the set.
	// This ensures that each edge name is unique within the graph.
	// If a duplicate edge name is found, return an error indicating the name is not unique.
	for _, v := range db.Edges {
		// if the edge name is already in the set, then return an error
		if _, ok := db.edgeNameSet[v.Name]; ok {
			return fmt.Errorf("edge name %s is not unique", v.Name)
		}
		db.edgeNameSet[v.Name] = void{}
	}
	return nil
}

// check if the node name is Exists
func (db *InMemoryDB) checkNodeNameExists(name string) bool {
	_, ok := db.nodeNameSet[name]
	return ok
}

// check if the edge name is Exists
func (db *InMemoryDB) checkEdgeNameExists(name string) bool {
	_, ok := db.edgeNameSet[name]
	return ok
}

// update the two NodeNameMap and EdgeNameMap bidimap by the Nodes and Edges
func (db *InMemoryDB) RegenerateBidimap() {
	db.NodeNameMap.Clear()
	db.EdgeNameMap.Clear()
	// iter all the nodes and edges and update the two bidimap
	for k, v := range db.Nodes {
		db.NodeNameMap.Put(k, v.Name)
	}

	for k, v := range db.Edges {
		db.EdgeNameMap.Put(k, v.Name)
	}
}

// ~ GraphDB Definition Section END
