package merkledag

import (
	"encoding/json"
	"hash"
	"strings"
)

func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	pathSegments := strings.Split(path, "/")
	if len(pathSegments) == 0 {
		return nil
	}

	objBytes, _ := store.Get(hash)

	var obj Object
	json.Unmarshal(objBytes, &obj)

	return recursiveSearch(store, obj, pathSegments, hp)
}

func recursiveSearch(store KVStore, obj Object, pathSegments []string, hp HashPool) []byte {
	if len(pathSegments) == 0 {
		return obj.Data
	}

	for _, value := range obj.Links {
		switch pathSegments[0] {
		case "blob":
			blobValue := CalHash2(value.Hash, hp.Get())
			blobData, _ := store.Get(blobValue)
			return blobData
		case "link":
			subObject := getObject(store, value.Hash)
			return recursiveSearch(store, subObject, pathSegments, hp)
		case "tree":
			subObject := getObject(store, value.Hash)
			return recursiveSearch(store, subObject, pathSegments[1:], hp)
		}
	}

	return nil
}

func CalHash2(data []byte, h hash.Hash) []byte {
	h.Reset()
	hash := h.Sum(data)
	h.Reset()
	return hash
}

func getObject(store KVStore, hash []byte) Object {
	objBytes, _ := store.Get(hash)

	var obj Object
	json.Unmarshal(objBytes, &obj)
	return obj
}
