package btree

import "github.com/Manik-Jasrai/ByteStore.git/utils"

func NodeSplit2(left BNode, right BNode, old BNode) {
	utils.Assert(old.nKeys() >= 2, "Too short to split : NodeSplit2")
	// initial guess
	nleft := old.nKeys() / 2
	// try to fit left
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	utils.Assert(nleft >= 1, "Empty Node Not Possible")
	// try to fit right
	right_bytes := func() uint16 {
		return old.nBytes() - left_bytes() + uint16(4)
	}

	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	utils.Assert(nleft < old.nKeys(), "")
	nright := old.nKeys() - nleft
	left.setHeader(old.bType(), nleft)
	right.setHeader(old.bType(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// the left may still be bigger
	utils.Assert(right.nBytes() <= BTREE_PAGE_SIZE, "Not Good")
}

func NodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	NodeSplit2(left, right, old)
	if left.nBytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	NodeSplit2(leftleft, middle, left)
	utils.Assert(leftleft.nBytes() <= BTREE_PAGE_SIZE, "Oversized data")
	return 3, [3]BNode{leftleft, middle, right}
}
