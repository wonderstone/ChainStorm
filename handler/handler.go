package handler

type Node interface {
	Export() map[string]interface{}
}

type Edge interface {
	Export() map[string]interface{}
}

// collection is not mandatory for node/vertex，but is good for node-management in categories
// it is the concept borrowed from arangodb
type GraphDB interface {
	// - Init operations
	Init(yamlPath string) error
	// - Connection operations
	Connect() error
	Disconnect() error

	// - CRUD operations
	// + Create operations
	AddNode(n Node) (id interface{},err error)
	AddEdge(e Edge) (id interface{},err error)
	// + Query operations
	GetNode(id interface{}) (Node, error)
	GetEdge(id interface{}) (Edge, error)
	GetFromNodes(id interface{}) ([]interface{}, error)
	GetToNodes(id interface{}) ([]interface{}, error)
	GetInEdges(id interface{}) ([]interface{}, error)
	GetOutEdges(id interface{}) ([]interface{}, error)
	// + Update operations
	UpdateNode(n Node) (id interface{},err error)
	UpdateEdge(e Edge) (id interface{},err error)
	// + Delete operations
	DeleteNode(id interface{}) error
	DeleteEdge(id interface{}) error

	// + Graph operations
	// - Traversal operations
	GetAllRelatedNodes(id interface{}) ([][]interface{}, error)
	GetAllRelatedNodesInEdgeSlice(id interface{}, EdgeSlice ...Edge) ([][]interface{}, error)
	GetAllRelatedNodesInRange(id interface{}, min, max int) ([][]interface{}, error)
}
