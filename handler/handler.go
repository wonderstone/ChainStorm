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
	// Connect to the database and update the information in the struct
	Connect() error
	// Disconnect from the database, some maynot need this
	Disconnect() error

	// - CRUD operations
	// + Create operations
	AddNode(n Node) (interface{}, error)
	AddEdge(e Edge) (interface{}, error)
	// + Update operations
	// - Replace: data field will be replaced
	// - The existing document is completely replaced by the new document. 
	// - Any fields that are not specified in the new document will be removed.
	ReplaceNode(n Node) error
	ReplaceEdge(e Edge) error
	// - Update: data field will be updated
	// - Only the specified fields in the update document are modified. 
	// - Fields that are not specified in the update document remain unchanged.
	UpdateNode(n Node) error
	UpdateEdge(e Edge) error
	// - Merge: data field will be merged
	// - The same fields in the document are added together
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
	// GetAllRelatedNodesInRange(name interface{}, max int) ([][]Node, error)
}
