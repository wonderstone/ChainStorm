package arango

// import (
// 	"testing"

// 	"github.com/wonderstone/chainstorm/base"
// )

// func TestInit(t *testing.T) {
// 	ag := ArangoGraph{}
// 	yamlPath := "config/config.yaml"
// 	err := ag.Init(yamlPath)
// 	if err != nil {
// 		t.Errorf("Error: %v", err)
// 	}

// 	// test connection
// 	err = ag.Connect()
// 	if err != nil {
// 		t.Errorf("Error: %v", err)
// 	}

// 	// test disconnection

// 	err = ag.Disconnect()

// 	if err != nil {
// 		t.Errorf("Error: %v", err)
// 	}

// 	// test AddNode
// 	node := base.Node{
// 		ID:         "1",
// 		Collection: "mycol",
// 		Name:       "test",
// 		Data:       map[string]interface{}{"a": "b"},
// 	}

// 	node2 := base.Node{
// 		ID:         "2",
// 		Collection: "mycol",
// 		Name:       "test",
// 		Data:       map[string]interface{}{"a": "b"},
// 	}

// 	err = ag.AddNode(&node)
// 	err = ag.AddNode(&node2)

// 	if err != nil {
// 		t.Errorf("Error: %v", err)
// 	}

// 	// test AddEdge
// 	edge := base.EdgeJSON{
// 		ID:         "2",
// 		Collection: "my_edge_collection",
// 		From:       "mycol/18787",
// 		To:         "mycol/18862",
// 		Data:       map[string]interface{}{"aaa": "baaa"},
// 	}

// 	err = ag.AddEdge(&edge)

// 	if err != nil {
// 		t.Errorf("Error: %v", err)
// 	}

// 	// test add graph
// 	err = ag.AddGraph()

// }

