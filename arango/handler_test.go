package arango

import (
	"context"
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
	err = ag.Connect()
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
}

func TestArangoGraph_CRUD(t *testing.T) {

	ctx := context.Background()

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
		Name:       "test1",
		Data:       map[string]interface{}{"a": "b"},
	}

	node2 := Node{	
		Collection: "persons",
		Name:       "test2",
		Data:       map[string]interface{}{"b": "c"},
	}
	node3 := Node{
		Collection: "movies",
		Name:       "test3",
		Data:       map[string]interface{}{"c": "d"},
	}


	meta, err := ag.AddNode(&node)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	meta2, err:= ag.AddNode(&node2)
	if err != nil {	
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	meta3, err:= ag.AddNode(&node3)
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

	edge3 := Edge{
		Collection: "likes",
		From:       meta2.(driver.DocumentMeta).ID.String(),
		To:         meta.(driver.DocumentMeta).ID.String(),
		Data:       map[string]interface{}{"aac": "bbc"},
	}

	edge4 := Edge{
		Collection: "likes",
		From:       meta2.(driver.DocumentMeta).ID.String(),
		To:         meta3.(driver.DocumentMeta).ID.String(),
		Data:       map[string]interface{}{"aac": "bbc"},
	}



	_, err = ag.AddEdge(&edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	_, err = ag.AddEdge(&edge2)
	if err == nil {
		t.Errorf("Test failed, expected Non-nil, got %v", err)
	}

	_, err = ag.AddEdge(&edge3)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}


	_, err = ag.AddEdge(&edge4)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}


	err = ag.createGraph()
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}



	// test replaceNode
	node.Data = map[string]interface{}{"a": "c"}
	err = ag.ReplaceNode(&node)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// delete the graph and all its nodes and edges
	options := driver.RemoveGraphOptions{
		DropCollections: true,
	}

	err = ag.graph.RemoveWithOpts(ctx,&options)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}



}