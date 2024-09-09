package local

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewNode(t *testing.T) {
	dp := filepath.Join("data", "company", "600001.json")
	// Read the json file and create a new node
	dt, err := ReadJSONFile(dp)
	if err != nil {
		t.Error(err)
	}
	node,_ := NewNode(WithNCollection("company"), WithNData(dt))
	// Check if the node is created correctly
	assert.Equal(t, "600001", node.ID)
	// add node companyEmployees field by 1 and assign back to it
	node.Data["companyEmployees"] = node.Data["companyEmployees"].(float64) + 1
	// write the node back to the json file
	err = WriteJSONFile(dp, node.Export())
	if err != nil {
		t.Error(err)
	}

	// based on the node, create 10 new nodes in the same collection
	// every new node change the "ID" field to a new value
	for i := 0; i < 10; i++ {
		node,_ := NewNode(WithNCollection("company"), WithNData(dt))
		node.Data["ID"] = "60000" + strconv.Itoa(i)
		node.ID = "60000" + strconv.Itoa(i)
		// write the node back to the json file
		err = WriteJSONFile(filepath.Join("data", "company", node.ID+".json"), node.Export())
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
		node,_ := NewNode(WithNCollection("company"), WithNData(dt))
		nodes[node.ID] = node
	}
	// create edges between the nodes
	for _, node := range nodes {
		for _, node2 := range nodes {
			if node.ID != node2.ID {
				edge,_ := NewEdge(
					WithEID(node.ID + "-" + node2.ID),
					WithECollection("invest"), 
					WithEFrom(node), 
					WithETo(node2), 
					WithEWeight(1),
					WithEData(map[string]interface{}{}))
				// write the edge to the file
				err := WriteJSONFile(filepath.Join("data", "invest", edge.ID+".json"), edge.Export())
				if err != nil {
					t.Error(err)
				}
			}
		}
	}



}

// test the InMemoryDB methods
func TestInMemoryDB(t *testing.T) {
	// initialize the in-memory database
	db := NewInMemoryDB()
	fp := filepath.Join("config", "config.yaml")
	db.Init(fp)
	// connect to the database
	err := db.Connect()
	if err != nil {
		t.Error(err)
	}

	// disconnect from the database
	err = db.Disconnect()
	if err != nil {
		t.Error(err)
	}

	// Test BFSWithLevels
	res:= db.BFSWithLevels("600001")
	fmt.Println(res)
	assert.Equal(t, 2, len(res))
	// Test BFSWithLevelsStruct
	resStruct := db.BFSWithLevelsStruct("600001")
	for _, v := range resStruct {
		fmt.Println(v.Node.ID,v.Level)
	}
	assert.Equal(t, 10, len(resStruct))
	// TEST BFSWithPaths

	resPaths := db.BFSWithPaths("600001")
	for _, v := range resPaths {
		fmt.Println(v.Node.ID,v.Path)
	}
	assert.Equal(t, 10, len(resPaths))
	// Test BFSWithWeightRange
	res = db.BFSWithWeightRange("600001", 0, 0)
	fmt.Println(res)
	assert.Equal(t, 1, len(res))

	// TEst DFSWithLevelsStruct
	tmp := db.DFS("600001")
	tmp1 := db.DFSWithCompletePaths("600001")
	tmp2 := db.DFSWithLevelsStruct("600001")
	tmp3 := db.DFSWithPaths("600001")

	fmt.Println(tmp)
	fmt.Println(tmp1)
	fmt.Println(tmp2)
	fmt.Println(tmp3)


}

func TestAddVertex(t *testing.T) {
	db := NewInMemoryDB()
	collection := "company"
	data := map[string]interface{}{
		"companyName":      "Google",
		"companyEmployees": 1000,
	}

	id, err := db.AddVertex(collection, data)
	if err != nil {
		t.Error(err)
	}

	// Verify that the vertex is added correctly
	vertex, err := db.GetVertexDB(id)
	if err != nil {
		t.Error(err)
	}

	expectedData := map[string]interface{}{
		"ID":               id,
		"Collection":       collection,
		"companyName":      "Google",
		"companyEmployees": 1000,
	}

	assert.Equal(t, expectedData, vertex)
}
