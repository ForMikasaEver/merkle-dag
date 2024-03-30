package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	switch node.Type() {
	case FILE:
		file := node.(File)
		obj_value, _ := json.Marshal(StoreFile(store, file, h))
		hash := CalHash1(obj_value, h)
		return hash
	case DIR:
		dir := node.(Dir)
		obj_value, _ := json.Marshal(StoreDir(store, dir, h))
		hash := CalHash1(obj_value, h)
		return hash
	}
	return nil
}

func CalHash1(data []byte, h hash.Hash) []byte {
	h.Reset()
	hash := h.Sum(data)
	h.Reset()
	return hash
}

func StoreFile(store KVStore, file File, h hash.Hash) *Object {
	data := file.Bytes()
	blob := Object{Data: data, Links: nil}
	obj_value, _ := json.Marshal(blob)
	hash := CalHash1(obj_value, h)
	store.Put(hash, data)
	return &blob
}

func StoreDir(store KVStore, dir Dir, h hash.Hash) *Object {
	it := dir.It()
	treeObject := &Object{}
	for it.Next() {
		n := it.Node()
		switch n.Type() {
		case FILE:
			file := n.(File)
			tmp := StoreFile(store, file, h)
			obj_value, _ := json.Marshal(tmp)
			hash := CalHash1(obj_value, h)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: hash,
				Size: int(file.Size()),
				Name: file.Name(),
			})
			typeName := "link"
			if tmp.Links == nil {
				typeName = "blob"
			}
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)
		case DIR:
			dir := n.(Dir)
			obj_value, _ := json.Marshal(StoreDir(store, dir, h)) // Recursion
			hash := CalHash1(obj_value, h)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: hash,
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			typeName := "tree"
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)
		}
	}
	obj_value, _ := json.Marshal(treeObject)
	hash := CalHash1(obj_value, h)
	store.Put(hash, obj_value)

	return treeObject
}
