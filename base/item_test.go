package base

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewNode(t *testing.T) {
	dp := filepath.Join("data", "company", "600001.json")
	// Read the json file and create a new node
	dt, err := ReadJSONFile(dp)
	if err != nil {
		t.Error(err)
	}

	uid := uuid.New().String()

	node, _ := NewNode( WithNName("600001"), WithNCollection("company"), WithNData(dt))
	// Check if the node is created correctly
	assert.Equal(t, uid, node.ID)
	// add node companyEmployees field by 1 and assign back to it
	node.Data["companyEmployees"] = node.Data["companyEmployees"].(float64) + 1
	// write the node back to the json file
	err = WriteJSONFile(dp, node.Export())
	if err != nil {
		t.Error(err)
	}
	// output the node
	err = WriteJSONFile(filepath.Join("data", "company", node.Name+".json"), node.Export())
	
	if err != nil {
		t.Error(err)
	}


	dp = filepath.Join("data", "company", node.Name+".json")
	dt, err = ReadJSONFile(dp)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(dt)

	// based on the node, create 10 new nodes in the same collection
	// every new node change the "ID" and "Name" field to a new value
	for i := 0; i < 5; i++ {
		uid = uuid.New().String()
		node, _ = NewNode(

			WithNName(fmt.Sprintf("60000%d", i)),
			WithNCollection("company"),
			WithNData(dt))
		err = WriteJSONFile(
			filepath.Join("data", "company", node.Name+".json"),
			node.Export())
		if err != nil {
			t.Error(err)
		}
	}

}

// test the NewEdge function based on data/company dir
// the edge relationship is "invest" as the collection
// the edge from and to are the
func TestNewEdge(t *testing.T) {
	// read all json in the data/company dir and create nodes
	dp := filepath.Join("data", "company")

	files, err := os.ReadDir(dp)
	if err != nil {
		t.Error(err)
	}
	// create a map to store all the nodes
	nodes := make(map[string]*Node)
	for _, file := range files {
		dt, err := ReadJSONFile(filepath.Join(dp, file.Name()))
		if err != nil {
			t.Error(err)
		}
		node, _ := NewNode(
			WithNName(dt["Name"].(string)),
			WithNCollection("company"),
			WithNData(dt))
		nodes[node.ID] = node
	}
	// create edges between the nodes
	for _, node := range nodes {
		for _, node2 := range nodes {
			if node.ID != node2.ID {
				edge,_ := NewEdge(
					WithEID(uuid.New().String()),
					WithEName(fmt.Sprintf("%s-%s", node.Name, node2.Name)),
					WithECollection("invest"),
					WithEFrom(node),
					WithETo(node2),
					WithEWeight(1),
					WithEData(map[string]interface{}{}))
				// write the edge to the file
				err := WriteJSONFile(filepath.Join("data", "invest", edge.Name+".json"), edge.Export())
				if err != nil {
					t.Error(err)
				}
			}
		}
	}

}