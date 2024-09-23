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
	AddNode(n Node) (interface{}, error)
	AddEdge(e Edge) (interface{}, error)
	// + Update operations
	ReplaceNode(n Node) error
	ReplaceEdge(e Edge) error
	UpdateNode(n Node) error
	UpdateEdge(e Edge) error
	MergeNode(n Node) error
	MergeEdge(e Edge) error
	// + Delete operations
	DeleteNode(name interface{}) error
	DeleteItemByID(id interface{}) error

	// + Query operations
	GetItemByID(id interface{}) (interface{}, error)
	GetNode(name interface{}) (Node, error)
	GetNodesByRegex(regex string) ([]Node, error)
	GetEdgesByRegex(regex string) ([]Edge, error)

	GetFromNodes(name interface{}) ([]Node, error)
	GetToNodes(name interface{}) ([]Node, error)
	GetInEdges(name interface{}) ([]Edge, error)
	GetOutEdges(name interface{}) ([]Edge, error)

	// + Graph operations
	// - Traversal operations
	GetAllRelatedNodes(name interface{}) ([][]Node, error)
	GetAllRelatedNodesInEdgeSlice(name interface{}, EdgeSlice ...Edge) ([][]Node, error)
	GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)
}
