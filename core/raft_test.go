package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

func TestGob(t *testing.T) {

	peers := map[NodeId]NodeAddr{
		"node1":"192.16.89.30",
	}

	enData, enErr := encode(peers)
	if enErr != nil {
		t.Errorf("序列化失败")
	}

	var newMap map[NodeId]NodeAddr
	deErr := decode(&newMap, enData)
	if deErr != nil {
		t.Errorf("%s", fmt.Errorf("反序列化失败%w", deErr))
	}

	for id, addr := range newMap {
		fmt.Printf("%s, %s\n", id, addr)
	}
}

func encode(data interface{}) ([]byte, error) {
	var enData bytes.Buffer
	encoder := gob.NewEncoder(&enData)
	enErr := encoder.Encode(data)
	return enData.Bytes(), enErr
}

func decode(data interface{}, enData []byte) error {
	decoder := gob.NewDecoder(bytes.NewBuffer(enData))
	return decoder.Decode(data)
}