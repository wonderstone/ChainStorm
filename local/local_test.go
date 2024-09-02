package local

import (
	"path/filepath"
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
	node := NewNode(WithNCollection("company"), WithNData(dt))
	// Check if the node is created correctly
	assert.Equal(t, "600001", node.ID)
	// add node companyEmployees field by 1 and assign back to it
	node.Data["companyEmployees"] = node.Data["companyEmployees"].(float64) + 1
	// write the node back to the json file
	err = WriteJSONFile(dp, node.Export())
	if err != nil {
		t.Error(err)
	}
}

// test the InMemoryDB methods
func TestInMemoryDB(t *testing.T) {
	// initialize the in-memory database
	db := NewInMemoryDB()
	fp := filepath.Join("config", "config.yaml")
	db.Init(fp)

	// create a new node
	collection := "company"

	node := NewNode(
		WithNCollection(collection),
		WithNData(map[string]interface{}{
			"companyName":      "Google",
			"companyEmployees": 1000,
		}))

	tmpdb := map[string]interface{}{
		"ID":               "600021",
		"companyEmployees": 2000,
	}

	db.AddVertex(collection, node.Data)
	db.AddVertex(collection, tmpdb)

	dp := filepath.Join("data", "company", "600001.json")
	dt, _ := ReadJSONFile(dp)

	db.AddVertex(collection, dt)

}
