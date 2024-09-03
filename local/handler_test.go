package local

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadJSONFile(t *testing.T) {
	// the path of the data/company/600001.json file
	dp := filepath.Join("data", "company", "600001.json")

	// Call the ReadJSONFile function with the path of the temporary file
	result, err := ReadJSONFile(dp)
	if err != nil {
		t.Fatalf("Failed to read JSON file: %v", err)
	}

	// Check if the result is as expected
	assert.Equal(t, "600001", result["ID"])
	// change the result["companyEmployees"] to 1001 and write it back to the file
	result["companyEmployees"] = result["companyEmployees"].(float64) +1
	//+ add WriteJSONFile test

	err = WriteJSONFile(dp, result)
	if err != nil {
		t.Fatalf("Failed to write JSON file: %v", err)
	}

}

func TestHandlerInterface(t *testing.T) {
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