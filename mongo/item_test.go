package mongo

import (
	"fmt"
	"testing"
)

func TestInit(t *testing.T) {
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

// test the Connect and Disconnect methods

func TestConnect(t *testing.T) {
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	err = mg.Connect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	err = mg.Disconnect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}
}

// test the AddNode  and AddEdge methods

func TestAddNode(t *testing.T) {
	mg := MongoGraph{}
	yamlPath := "config/config.yaml"
	err := mg.Init(yamlPath)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	err = mg.Connect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}



	node := Node{
		Collection: "Company",
		Name: 	 "Google",
		Data: map[string]interface{}{
			"location": "Mountain View",
			"employees": 1000,
		},
	}
	res, err := mg.AddNode(node)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	fmt.Println(res)


	n,err := mg.GetNode(
		map[string]interface{}{
			"Name": "Google",
			"Collection": "Company"})
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if n.Name != "Google" {
		t.Errorf("Error: %v", err)
	}


	err = mg.Disconnect()
	if err != nil {
		t.Errorf("Error: %v", err)
	}


}



