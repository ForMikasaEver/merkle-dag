package merkledag

import (
	"encoding/json"
	"hash"
)

const (
	K          = 1 << 10
	BLOCK_SIZE = 256 * K
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
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	switch node.Type() {
	case FILE:
		StoreFile(store, node.(File), h)
		break
	case DIR:
		StoreDir(store, node.(Dir), h)
		break
	}
	return nil
}

func StoreFile(store KVStore, node File, h hash.Hash) []byte {
	var t []byte

	if node.Size() > BLOCK_SIZE {
		t = []byte("list")
	} else {
		t = []byte("blob")
	}

	_ = t // 使用 t 变量，以避免 "unused" 错误

	data := node.Bytes()
	h.Reset()
	h.Write(data)
	hash := h.Sum(nil)

	store.Put(hash, data)

	return hash
}

func StoreDir(store KVStore, dir Dir, h hash.Hash) []byte {
	tree := Object{
		Links: make([]Link, 0),
		Data:  nil,
	}

	it := dir.It()

	for it.Next() {
		node := it.Node()

		var hash []byte
		if node.Type() == FILE {
			hash = StoreFile(store, node.(File), h)
		} else if node.Type() == DIR {
			hash = StoreDir(store, node.(Dir), h)
		}

		link := Link{Name: node.Name(), Hash: hash, Size: int(node.Size())}
		tree.Links = append(tree.Links, link)
	}

	treeData, _ := json.Marshal(tree)
	h.Reset()
	h.Write(treeData)
	treeHash := h.Sum(nil)
	store.Put(treeHash, treeData)

	return treeHash
}
