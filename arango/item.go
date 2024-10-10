package arango

import (
	"encoding/json"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/rs/zerolog"

	"github.com/emirpasic/gods/maps/hashbidimap"
)

// - ArangoDB 的 _id 由 collection/key 组成
// - _rev: 是 auto-generated revision string, 用于检测数据是否被修改, 无需人工干预
// - Node 结构体包含 ID、Collection、Name 和 Data 字段, 对应关系特殊
// omitempty 表示如果字段为空，则不输出到 JSON 字符串中
// "-" 表示不输出到 JSON 字符串中
type Node struct {
	ID         string                 `json:"_id"`
	Collection string                 `json:"collection"`
	Name       string                 `json:"name"`
	Data       map[string]interface{} `json:"data,omitempty"`
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

// CustomMarshalJSON is the custom marshal function for the Node struct
// in which the json file _id has collection/key format
func (n *Node) CustomMarshalJSON() ([]byte, error) {
	type Alias Node
	infos := strings.Split(n.ID, "/")

	return json.Marshal(&struct {
		*Alias
		Key string `json:"_key"`
	}{
		Alias: (*Alias)(n),
		Key:   infos[1],
	})
}

// CustomUnmarshalJSON is the custom unmarshal function for the Node struct
// in which the json file _id has collection/key format
func (n *Node) CustomUnmarshalJSON(data []byte) error {

	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}

	n.Collection = strings.Split(n.ID, "/")[0]
	return nil
}

type Edge struct {
	ID           string                 `json:"_id"`
	Relationship string                 `json:"relationship"`
	Collection   string                 `json:"-"`
	From         string                 `json:"_from"`
	To           string                 `json:"_to"`
	Data         map[string]interface{} `json:"data,omitempty"`
}

// implement the handler Edge interface
func (e *Edge) Export() map[string]interface{} {
	return map[string]interface{}{
		"_id":          e.ID,
		"relationship": e.Relationship,
		"collection":   e.Collection,
		"_from":        e.From,
		"_to":          e.To,
		"data":         e.Data,
	}
}

//	CustomMarshalJSON is the custom marshal function for the Edge struct
//
// in which the json file _id has collection/key format
func (e *Edge) CustomMarshalJSON() ([]byte, error) {
	type Alias Edge
	infos := strings.Split(e.ID, "/")

	return json.Marshal(&struct {
		*Alias
		Key string `json:"_key"`
	}{
		Alias: (*Alias)(e),
		Key:   infos[1],
	})
}

// CustomUnmarshalJSON is the custom unmarshal function for the Edge struct
// in which the json file _id has collection/key format
func (e *Edge) CustomUnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &e); err != nil {
		return err
	}

	e.Collection = strings.Split(e.ID, "/")[0]
	return nil
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
