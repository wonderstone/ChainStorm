package arango

import (
	driver "github.com/arangodb/go-driver"
	"github.com/rs/zerolog"

	"github.com/emirpasic/gods/maps/hashbidimap"
)

type Node struct {
	ID         string                 `bson:"_id,omitempty"`
	Collection string                 `bson:"collection"`
	Name       string                 `bson:"name"`
	Data       map[string]interface{} `bson:"data"`
}

// implement the handler Node interface
func (n *Node) Export() map[string]interface{} {
	return map[string]interface{}{
		"_id":        n.ID,
		"collection": n.Collection,
		"name":       n.Name,
		"data":       n.Data,
	}
}

type Edge struct {
	ID           string                 `bson:"_id,omitempty"`
	Relationship string                 `bson:"relationship"`
	Collection   string                 `bson:"collection"`
	From         string                 `bson:"from"`
	To           string                 `bson:"to"`
	Data         map[string]interface{} `bson:"data"`
}

// implement the handler Edge interface
func (e *Edge) Export() map[string]interface{} {
	return map[string]interface{}{
		"_id":          e.ID,
		"relationship": e.Relationship,
		"collection":   e.Collection,
		"from":         e.From,
		"to":           e.To,
		"data":         e.Data,
	}
}

// ArangoGraph is the struct for the ArangoDB
type ArangoGraph struct {
	username string
	password string
	server   string
	port     int
	dbname   string

	Client    driver.Client
	graphname string

	db    driver.Database
	graph driver.Graph

	// bidimap section for the node name and id
	nodeNameToIDMap *hashbidimap.Map

	logger *zerolog.Logger
}

func NewArangoGraph() (*ArangoGraph, error) {
	db := &ArangoGraph{}
	return db, nil
}
