package arango

import (
	"context"
	"fmt"
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

	meta2, err := ag.AddNode(&node2)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	meta3, err := ag.AddNode(&node3)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	// add an edge
	edge := Edge{
		Collection: "knows",
		From:       meta.(driver.DocumentMeta).ID.String(),
		To:         meta2.(driver.DocumentMeta).ID.String(),
		Relationship: "test000111",
		Data:       map[string]interface{}{"aa": "bb"},
	}

	edge2 := Edge{
		Collection: "knows",
		From:       "persons/alice",
		To:         meta.(driver.DocumentMeta).ID.String(),
		Relationship: "test000222",
		Data:       map[string]interface{}{"aa": "bb"},
	}

	edge3 := Edge{
		Collection: "likes",
		From:       meta2.(driver.DocumentMeta).ID.String(),
		To:         meta.(driver.DocumentMeta).ID.String(),
		Relationship: "test000333",
		Data:       map[string]interface{}{"aac": "bbc"},
	}

	edge4 := Edge{
		Collection: "likes",
		From:       meta2.(driver.DocumentMeta).ID.String(),
		To:         meta3.(driver.DocumentMeta).ID.String(),
		Data:       map[string]interface{}{"aac": "bbc"},
	}

	metaE, err := ag.AddEdge(&edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	edge.ID = metaE.(driver.DocumentMeta).ID.String()

	// ~ this one should fail for the from field is not in database
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

	// test replaceEdge
	edge.Data = map[string]interface{}{"aa": "cc"}
	err = ag.ReplaceEdge(&edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// test updateNode
	node.Data = map[string]interface{}{"abcd": "d"}
	err = ag.UpdateNode(&node)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// test updateEdge
	edge.Data = map[string]interface{}{"abcd": "d"}

	err = ag.UpdateEdge(&edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// test mergeNode
	node.Data = map[string]interface{}{"a": "cccc", "abcd": "d", "efg": "h"}

	err = ag.MergeNode(&node)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// test mergeEdge
	edge.Data = map[string]interface{}{"abcd": "d", "efg": "h"}

	err = ag.MergeEdge(&edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// test getNode
	_, err = ag.GetNode(node.Name)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// test GetItemByID
	_, err = ag.GetItemByID(meta.(driver.DocumentMeta).ID)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// !GetNodesByRegex
	ns, err := ag.GetNodesByRegex(".*test.*")
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	if len(ns) != 3 {
		t.Errorf("Test failed, expected 3, got %v", len(ns))
	}

	// GetEdgesByRegex
	es, err := ag.GetEdgesByRegex(".*test.*")
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	if len(es) != 2 {
		t.Errorf("Test failed, expected 2, got %v", len(es))
	}

	// GetFromNodes
	// ns, err := ag.GetFromNodes(node2.Name)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// if len(ns) != 2 {
	// 	t.Errorf("Test failed, expected 2, got %v", len(ns))
	// }

	// // GetToNodes
	// ns, err = ag.GetToNodes(node.Name)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// if len(ns) != 2 {
	// 	t.Errorf("Test failed, expected 2, got %v", len(ns))
	// }

	// // GetInEdges
	// es, err := ag.GetInEdges(node.Name)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// if len(es) != 2 {
	// 	t.Errorf("Test failed, expected 2, got %v", len(es))
	// }

	// // GetOutEdges
	// es, err = ag.GetOutEdges(node2.Name)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// if len(es) != 2 {
	// 	t.Errorf("Test failed, expected 2, got %v", len(es))
	// }

	// ~ delete the graph and all its nodes and edges
	options := driver.RemoveGraphOptions{
		DropCollections: true,
	}

	err = ag.graph.RemoveWithOpts(ctx, &options)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

}



// test the query generator
func TestQueryGenerator(t *testing.T) {
	// + connect to the database
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
	// + test the query generator
	q := map[string]interface{}{
		// # 
		"collections":  []string{"persons", "movies"},
		"regexPatterns": []string{".*test.*", ".*d.*"},
		// "filter": "REGEX_MATCHES(node.name, @regex0, true)",
		"filter": `
            REGEX_MATCHES(node.name, @regex0, true) AND
            (
                IS_STRING(node.data['abcd']) AND
                REGEX_MATCHES(node.data['abcd'], @regex1, true)
            )
        `,
		"query": `
			FOR node IN %s
			FILTER %s
			RETURN node
		`,
	}

	query, bindVars, err := QueryGenerator(q)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	fmt.Println(query)

	is, err :=ag.Query(query,bindVars)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	fmt.Println(is)

	var result []interface{}
	// test the length of the nodes
	for _, i := range is {
		if _,ok := i.(map[string]interface{})["_from"]; ok {
			var edge Edge
			err := mapToStruct(i.(map[string]interface{}), &edge)
			if err != nil {
				t.Errorf("Test failed, expected nil, got %v", err)
			}

			result = append(result, edge)

		} else {
			var node Node
			err := mapToStruct(i.(map[string]interface{}), &node)
			if err != nil {
				t.Errorf("Test failed, expected nil, got %v", err)
			}

			result = append(result, node)

		}
	}


	if len(result) != 3 {
		t.Errorf("Test failed, expected 3, got %v", len(is))
	}
}


