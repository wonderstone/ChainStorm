package handler

// collection is not mandatory for node/vertex，but is good for node-management in categories
// it is the concept borrowed from arangodb
type GraphDB interface {
	// + Init operations
	Init(yamlPath string) error
	// + Connection operations
	Connect() error
	Disconnect() error




	// + CRUD operations
	// + Create operations
	AddVertex(collection string, db map[string]interface{}) (id string, err error)
	AddEdge(collection string, from, to string, db map[string]interface{}) (id string, err error)
	// + Query operations
	GetVertexDB(id string) (map[string]interface{}, error)
	GetEdgeDB(id string) (map[string]interface{}, error)
	GetFromVertices(id string) ([]string, error)
	GetToVertices(id string) ([]string, error)
	GetInEdges(id string) ([]string, error)
	GetOutEdges(id string) ([]string, error)
	// + Update operations
	UpdateVertex(id string, db map[string]interface{}) error
	UpdateEdge(id, from, to string, db map[string]interface{}) error
	// + Delete operations
	DeleteVertex(id string) error
	DeleteEdge(id string) error

	// + Graph operations
	// - Traversal operations
	GetAllRelatedVertices(id string) ([]string, error)
	GetAllRelatedVerticesInEdgeCollectiosn(id string, collections ...string) ([]string, error)
	GetAllRelatedVerticesInRange(id string, min, max int) ([]string, error)

	// - Shortest path operations
	ShortestPath(from, to string) ([]string, error)
	ShortestPathInEdgeCollections(from, to string, collections ...string) ([]string, error)
	ShortestPathInRange(from, to string, min, max int) ([]string, error)

	// - Custom operations
	CustomQuery(query string) ([]string, error)
}





