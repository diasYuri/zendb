package storage

import (
	"bytes"
	"encoding/binary"
)

// | type | nkeys | pointers |  offsets  | key-values |
// |  2B  |  2B   | nkeys*8B |  nkeys*2B |     ...    |

// key-values
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

const (
	HEADER             = 4
	POINTER_SIZE       = 8
	OFFSET_SIZE        = 8
	HEADER_KV          = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

type BNode struct {
	data []byte
}

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	pos := getPtrPos(idx)
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := getPtrPos(idx)
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

func getPtrPos(idx uint16) uint16 {
	return HEADER + POINTER_SIZE*idx
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	offsetPos := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node.data[offsetPos:])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*(idx-1)
}

// key-value
func (node BNode) kvPos(idx uint16) uint16 {
	return HEADER + POINTER_SIZE*node.nkeys() + OFFSET_SIZE*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+HEADER_KV:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+HEADER_KV+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// todo: bisert
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

func leafInsert(new, old BNode, key, val []byte, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func nodeAppendRange(new, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	if n == 0 {
		return
	}

	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)

	for i := uint16(1); i <= n; i++ {
		offset := dstBegin + old.getOffset(srcOld+1) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	//KV
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key, val []byte) {
	new.setPtr(idx, ptr)

	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)

	new.setOffset(idx+1, new.getOffset(idx)+HEADER_KV+uint16(len(key)+len(val)))
}

type BTree struct {
	root uint64
	get  func(uint64) BNode
	new  func(BNode) uint64
	del  func(uint64)
}

func treeInsert(tree *BTree, node BNode, key, val []byte) {
	new := BNode{
		data: make([]byte, 2*BTREE_PAGE_SIZE),
	}

	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, key, val, idx+1)
		}
	case BNODE_NODE:
		//nodeInsert(tree, new, node, key, val, idx)
	}
}

func nodeInsert(tree *BTree, new, node BNode, key, val []byte, idx uint16) {
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)

	nsplit, splited := nodeSplit3(knode)
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	// the initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	leftBytes := func() uint16 {
		return HEADER + POINTER_SIZE*nleft + 2*nleft + old.getOffset(nleft)
	}

	for leftBytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	// try to fit the right half
	rightBytes := func() uint16 {
		return old.nbytes() - leftBytes() + HEADER
	}
	for rightBytes() > BTREE_PAGE_SIZE {
		nleft++
	}

	nright := old.nkeys() - nleft

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)}
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}

	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right}
}

func nodeReplaceKidN(tree *BTree, new, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func init() {
	//nodemax := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	//assert(nodemax <= BTREE_PAGE_SIZE)
}
