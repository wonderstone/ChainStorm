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

	// $ Connect to the database
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

	// $ Node test case
	nodeTests := []struct {
		name string
		node Node
		meta driver.DocumentMeta
	}{
		{
			name: "AddNode test1",
			node: Node{
				Collection: "persons",
				Name:       "test1",
				Data:       map[string]interface{}{"a": "b"},
			},
		},
		{
			name: "AddNode test2",
			node: Node{
				Collection: "persons",
				Name:       "test2",
				Data:       map[string]interface{}{"b": "c"},
			},
		},
		{
			name: "AddNode test3",
			node: Node{
				Collection: "movies",
				Name:       "test3",
				Data:       map[string]interface{}{"c": "d"},
			},
		},
	}

	// - Add nodes and store metadata
	for i := range nodeTests {
		tt := &nodeTests[i]
		t.Run(tt.name, func(t *testing.T) {
			meta, err := ag.AddNode(&tt.node)
			if err != nil {
				t.Errorf("Test failed, expected nil, got %v", err)
			}
			tt.meta = meta.(driver.DocumentMeta)
		})
	}

	// $ Edge test case
	edgeTests := []struct {
		name       string
		edge       Edge
		shouldFail bool
	}{
		{
			name: "AddEdge test1",
			edge: Edge{
				Collection:   "knows",
				From:         nodeTests[0].meta.ID.String(),
				To:           nodeTests[1].meta.ID.String(),
				Relationship: "knows in school as classmate",
				Data:         map[string]interface{}{"aa": "bb"},
			},
			shouldFail: false,
		},
		{
			name: "AddEdge test2",
			edge: Edge{
				Collection:   "knows",
				From:         "persons/alice",
				To:           nodeTests[0].meta.ID.String(),
				Relationship: "knows in club where alice is a lap dancer",
				Data:         map[string]interface{}{"aa": "bb"},
			},
			shouldFail: true,
		},
		{
			name: "AddEdge test3",
			edge: Edge{
				Collection:   "likes",
				From:         nodeTests[1].meta.ID.String(),
				To:           nodeTests[0].meta.ID.String(),
				Relationship: "well, who doesn't like test1",
				Data:         map[string]interface{}{"aac": "bbc"},
			},
			shouldFail: false,
		},
		{
			name: "AddEdge test4",
			edge: Edge{
				Collection:   "likes",
				From:         nodeTests[1].meta.ID.String(),
				To:           nodeTests[2].meta.ID.String(),
				Relationship: "watched this movie with partner, so more than just movie but also memory",
				Data:         map[string]interface{}{"aac": "bbc"},
			},
			shouldFail: false,
		},
	}

	// - Add edges and check results
	for i, et := range edgeTests {
		t.Run(et.name, func(t *testing.T) {
			meta, err := ag.AddEdge(&et.edge)
			if et.shouldFail {
				if err == nil {
					t.Errorf("Test failed, expected Non-nil, got %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Test failed, expected nil, got %v", err)
				} else {
					edgeTests[i].edge.ID = meta.(driver.DocumentMeta).ID.String()
				}
			}
		})
	}

	// $ Create graph
	err = ag.createGraph()
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// $ Update part

	// - replaceNode:
	nodeTests[0].node.Data = map[string]interface{}{"a": "c"}
	err = ag.ReplaceNode(&nodeTests[0].node)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// - replaceEdge
	edgeTests[0].edge.Data = map[string]interface{}{"aa": "cc"}
	err = ag.ReplaceEdge(&edgeTests[0].edge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// - updateNode
	// new node
	tmpNode := Node{
		Name: "test1",
		Data: map[string]interface{}{"abcd": "d"},
	}
	// ! original node data field is {"a": "c"} is untouched
	err = ag.UpdateNode(&tmpNode)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// - updateEdge
	// new edge
	tmpEdge := Edge{
		// ! must have the edge ID, From, To!!!!!
		ID:   edgeTests[0].edge.ID,
		From: edgeTests[0].edge.From,
		To:   edgeTests[0].edge.To,

		Data: map[string]interface{}{"abcd": "d"},
	}
	// ! original edge data field is {"aa": "cc"} is untouched
	err = ag.UpdateEdge(&tmpEdge)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}

	// // test updateEdge
	// edge.Data = map[string]interface{}{"abcd": "d"}

	// err = ag.UpdateEdge(&edge)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// // test mergeNode
	// node.Data = map[string]interface{}{"a": "cccc", "abcd": "d", "efg": "h"}

	// err = ag.MergeNode(&node)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// // test mergeEdge
	// edge.Data = map[string]interface{}{"abcd": "d", "efg": "h"}

	// err = ag.MergeEdge(&edge)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// // test getNode
	// _, err = ag.GetNode(node.Name)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// // test GetItemByID
	// _, err = ag.GetItemByID(meta.(driver.DocumentMeta).ID)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

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
	// ns, err = ag.GetFromNodes(node2.Name)
	// if err != nil {
	// 	t.Errorf("Test failed, expected nil, got %v", err)
	// }

	// if len(ns) != 1 {
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
		"collections":   []string{"persons", "movies"},
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

	is, err := ag.Query(query, bindVars)
	if err != nil {
		t.Errorf("Test failed, expected nil, got %v", err)
	}
	fmt.Println(is)

	var result []interface{}
	// test the length of the nodes
	for _, i := range is {
		if _, ok := i.(map[string]interface{})["_from"]; ok {
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
