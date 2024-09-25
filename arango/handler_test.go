package arango

import (
	"testing"

	"github.com/arangodb/go-driver"
)

func TestArangoGraph_Init(t *testing.T) {
	ag := ArangoGraph{}
	yamlPath := "config/config.yaml"
	err := ag.Init(yamlPath)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
}

func TestArangoGraph_Connect(t *testing.T) {
	ag := ArangoGraph{}
	yamlPath := "config/config.yaml"
	err := ag.Init(yamlPath)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	err = ag.Connect()
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	// // test the createGraph
	// err = ag.createGraph()
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// add a node
	node := Node{
		Collection: "persons",
		Name:       "test",
		Data:       map[string]interface{}{"a": "b"},
	}

	node2 := Node{	
		Collection: "persons",
		Name:       "test2",
		Data:       map[string]interface{}{"b": "c"},
	}

	meta, err := ag.AddNode(&node)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	meta2, err:= ag.AddNode(&node2)
	if err != nil {	
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// add an edge
	edge := Edge{
		Collection: "knows",
		From:       meta.(driver.DocumentMeta).ID.String(),
		To:         meta2.(driver.DocumentMeta).ID.String(),
		Data:       map[string]interface{}{"aa": "bb"},
	}


	edge2 := Edge{
		Collection: "knows",
		From:       "persons/alice",
		To:         meta.(driver.DocumentMeta).ID.String(),
		Data:       map[string]interface{}{"aa": "bb"},
	}

	_, err = ag.AddEdge(&edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	_, err = ag.AddEdge(&edge2)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}


	err = ag.createGraph()
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

}