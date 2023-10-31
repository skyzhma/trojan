package index

import (
	"bytes"
	"sort"
	"sync"
	"trojan/data"

	goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {

	art.lock.RLock()
	defer art.lock.RUnlock()
	pos, found := art.tree.Search(key)
	if !found {
		return nil
	}

	return pos.(*data.LogRecordPos)

}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {

	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()

	return deleted

}

func (art *AdaptiveRadixTree) Size() int {

	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size

}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {

	if art.tree == nil {
		return nil
	}

	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)

}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

type artIterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}

		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}

		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (arti *artIterator) Rewind() {
	arti.currIndex = 0
}

func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

func (arti *artIterator) Next() {
	arti.currIndex += 1
}

func (arti *artIterator) Valid() bool {
	return arti.currIndex < len(arti.values)
}

func (arti *artIterator) Key() []byte {
	return arti.values[arti.currIndex].key
}

func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currIndex].pos
}

func (arti *artIterator) Close() {
	arti.values = nil
}
