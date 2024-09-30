package arango

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
)

func TestNode_Export(t *testing.T) {
	node := &Node{
		ID:         "test_collection/123",
		Collection: "test_collection",
		Name:       "test_name",
		Data: map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		},
	}

	expected := map[string]interface{}{
		"_id":        "test_collection/123",
		"collection": "test_collection",
		"name":       "test_name",
		"data": map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		},
	}

	result := node.Export()
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Export() = %v, want %v", result, expected)
	}

	// 将 Node 标准实例序列化为 JSON 字符串
	jsonData, err := json.Marshal(node)
	if err != nil {
		panic(err)
	}
	fmt.Printf("序列化后的 JSON 字符串: %s\n", jsonData)

	// output the jsonData to local file
	file, err := os.OpenFile("node.json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = io.WriteString(file, string(jsonData))
	if err != nil {
		panic(err)
	}

	// 将 JSON 字符串反序列化为 Node 实例
	var newNode Node
	err = json.Unmarshal(jsonData, &newNode)
	if err != nil {
		panic(err)
	}
	fmt.Printf("反序列化后的 Node: %+v\n", newNode)
	// ~ 标准实例序列化和反序列化测试 会无法还原 因为反序列化后的 Node 实例的 Collection 字段为空
	// check if the two nodes are equal
	if reflect.DeepEqual(node, &newNode) {
		t.Errorf("反序列化后的Node中, Collection字段为空, 不再相同")
	}

	// 将 Node 自定义实例序列化为 JSON 字符串
	jsonData, err = node.CustomMarshalJSON()
	if err != nil {
		panic(err)
	}

	fmt.Printf("序列化后的 JSON 字符串: %s\n", jsonData)

	// output the jsonData to local file
	file, err = os.OpenFile("node_custom.json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	_, err = io.WriteString(file, string(jsonData))
	if err != nil {
		panic(err)
	}

	// 将 JSON 字符串反序列化为 Node 实例
	var newNodeCustom Node
	err = newNodeCustom.CustomUnmarshalJSON(jsonData)
	if err != nil {
		panic(err)
	}

	fmt.Printf("反序列化后的 Node: %+v\n", newNodeCustom)

	// check if the two nodes are equal
	if !reflect.DeepEqual(node, &newNodeCustom) {
		t.Errorf("反序列化后的Node与原始Node不相同")
	}

}

func TestEdge_Export(t *testing.T) {
	edge := &Edge{
		ID:           "test_collection/123",
		Relationship: "test_relationship",
		Collection:   "test_collection",
		From:         "test/from",
		To:           "test/to",
		Data: map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		},
	}

	expected := map[string]interface{}{
		"_id":          "test_collection/123",
		"relationship": "test_relationship",
		"collection":   "test_collection",
		"_from":        "test/from",
		"_to":          "test/to",
		"data": map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		},
	}

	result := edge.Export()
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Export() = %v, want %v", result, expected)
	}

	// 将 Edge 标准实例序列化为 JSON 字符串
	jsonData, err := json.Marshal(edge)
	if err != nil {
		panic(err)
	}
	fmt.Printf("序列化后的 JSON 字符串: %s\n", jsonData)

	// output the jsonData to local file
	file, err := os.OpenFile("edge.json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = io.WriteString(file, string(jsonData))
	if err != nil {
		panic(err)
	}

	// 将 JSON 字符串反序列化为 Edge 实例
	var newEdge Edge
	err = json.Unmarshal(jsonData, &newEdge)
	if err != nil {
		panic(err)
	}
	fmt.Printf("反序列化后的 Edge: %+v\n", newEdge)
	// ~ 标准实例序列化和反序列化测试 会无法还原 因为反序列化后的 Edge 实例的 Collection 字段为空
	// check if the two edges are equal
	if reflect.DeepEqual(edge, &newEdge) {
		t.Errorf("反序列化后的Edge中, Collection字段为空, 不再相同")
	}

	// 将 Edge 自定义实例序列化为 JSON 字符串
	jsonData, err = edge.CustomMarshalJSON()
	if err != nil {
		panic(err)
	}

	fmt.Printf("序列化后的 JSON 字符串: %s\n", jsonData)

	// output the jsonData to local file
	file, err = os.OpenFile("edge_custom.json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	_, err = io.WriteString(file, string(jsonData))
	if err != nil {
		panic(err)
	}

	// 将 JSON 字符串反序列化为 Edge 实例
	var newEdgeCustom Edge
	err = newEdgeCustom.CustomUnmarshalJSON(jsonData)

	if err != nil {
		panic(err)
	}

	fmt.Printf("反序列化后的 Edge: %+v\n", newEdgeCustom)

	// check if the two edges are equal
	if !reflect.DeepEqual(edge, &newEdgeCustom) {
		t.Errorf("反序列化后的Edge与原始Edge不相同")
	}

}

// CompareJSON 比较两个 JSON 字符串是否相等
func CompareJSON(json1, json2 string) (bool, error) {
	var obj1, obj2 interface{}

	// 反序列化第一个 JSON 字符串
	err := json.Unmarshal([]byte(json1), &obj1)
	if err != nil {
		return false, fmt.Errorf("反序列化 JSON1 失败: %v", err)
	}

	// 反序列化第二个 JSON 字符串
	err = json.Unmarshal([]byte(json2), &obj2)
	if err != nil {
		return false, fmt.Errorf("反序列化 JSON2 失败: %v", err)
	}

	// 使用 reflect.DeepEqual 比较两个对象
	return reflect.DeepEqual(obj1, obj2), nil
}
