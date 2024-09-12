package local

import (
	"fmt"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)


func regContain(targetStr,regStr string) bool{
	// define the regexp to contain the node name,
	// and maybe have some other characters before and after the node name
	// for example, the node name is 600001, the regexp can be `.*600001.*`
	regexp1 := regexp.MustCompile(".*"+regStr+".*")
	return regexp1.MatchString(targetStr)
}
// test the InMemoryDB methods
func TestInMemoryDB(t *testing.T) {
	// initialize the in-memory database
	db,err := NewInMemoryDB()
	if err != nil {
		t.Error(err)
	}
	
	fp := filepath.Join("config", "config.yaml")
	db.Init(fp)
	// connect to the database
	err = db.Connect()
	if err != nil {
		t.Error(err)
	}

	// GetNodeID by give the node name 600001
	// define the regexp to match the node name



	nodes, err := db.GetNodeIDs("60000",regContain)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(nodes)

	// disconnect from the database
	err = db.Disconnect()
	if err != nil {
		t.Error(err)
	}

	// Test BFSWithLevels
	res:= db.BFSWithLevels(nodes[0])
	fmt.Println(res)
	assert.Equal(t, 2, len(res))
	// Test BFSWithLevelsStruct
	// resStruct := db.BFSWithLevelsStruct("600001")
	// for _, v := range resStruct {
	// 	fmt.Println(v.Node.ID,v.Level)
	// }
	// assert.Equal(t, 10, len(resStruct))
	// // TEST BFSWithPaths

	// resPaths := db.BFSWithPaths("600001")
	// for _, v := range resPaths {
	// 	fmt.Println(v.Node.ID,v.Path)
	// }
	// assert.Equal(t, 10, len(resPaths))
	// Test BFSWithWeightRange
	res = db.BFSWithWeightRange(nodes[0], 0, 0)
	fmt.Println(res)
	assert.Equal(t, 1, len(res))

	// TEst DFSWithLevelsStruct
	// tmp := db.DFS("600001")
	// tmp1 := db.DFSWithCompletePaths("600001")
	// tmp2 := db.DFSWithLevelsStruct("600001")
	// tmp3 := db.DFSWithPaths("600001")

	// fmt.Println(tmp)
	// fmt.Println(tmp1)
	// fmt.Println(tmp2)
	// fmt.Println(tmp3)

}
