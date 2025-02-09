package tinybtree

const freeKey = -int64(^uint64(0) >> 1)
const maxItems = 31 // use an odd number
const minItems = maxItems * 40 / 100

type item struct {
	key   int64
	value interface{}
}

type node struct {
	numItems int
	items    [maxItems]item
	children [maxItems + 1]*node
}

// BTree is an ordered set of key/value pairs where the key is a string
// and the value is an interface{}
type BTree struct {
	height int
	root   *node
	length int
}

func (n *node) find(key int64) (index int, found bool) {
	i, j := 0, n.numItems
	for i < j {
		h := i + (j-i)/2
		if key >= n.items[h].key {
			i = h + 1
		} else {
			j = h
		}
	}
	if i > 0 && n.items[i-1].key >= key {
		return i - 1, true
	}
	return i, false
}

// Set or replace a value for a key
func (tr *BTree) Set(key int64, value interface{}) (
	prev interface{}, replaced bool,
) {
	if tr.root == nil {
		tr.root = new(node)
		tr.root.items[0] = item{key, value}
		tr.root.numItems = 1
		tr.length = 1
		return
	}
	prev, replaced = tr.root.set(key, value, tr.height)
	if replaced {
		return
	}
	if tr.root.numItems == maxItems {
		n := tr.root
		right, median := n.split(tr.height)
		tr.root = new(node)
		tr.root.children[0] = n
		tr.root.items[0] = median
		tr.root.children[1] = right
		tr.root.numItems = 1
		tr.height++
	}
	tr.length++
	return
}

func (n *node) split(height int) (right *node, median item) {
	right = new(node)
	median = n.items[maxItems/2]
	copy(right.items[:maxItems/2], n.items[maxItems/2+1:])
	if height > 0 {
		copy(right.children[:maxItems/2+1], n.children[maxItems/2+1:])
	}
	right.numItems = maxItems / 2
	if height > 0 {
		for i := maxItems/2 + 1; i < maxItems+1; i++ {
			n.children[i] = nil
		}
	}
	for i := maxItems / 2; i < maxItems; i++ {
		n.items[i] = item{}
	}
	n.numItems = maxItems / 2
	return
}

func (n *node) set(key int64, value interface{}, height int) (
	prev interface{}, replaced bool,
) {
	i, found := n.find(key)
	if found {
		prev = n.items[i].value
		n.items[i].value = value
		return prev, true
	}
	if height == 0 {
		for j := n.numItems; j > i; j-- {
			n.items[j] = n.items[j-1]
		}
		n.items[i] = item{key, value}
		n.numItems++
		return nil, false
	}
	prev, replaced = n.children[i].set(key, value, height-1)
	if replaced {
		return
	}
	if n.children[i].numItems == maxItems {
		right, median := n.children[i].split(height - 1)
		copy(n.children[i+1:], n.children[i:])
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = median
		n.children[i+1] = right
		n.numItems++
	}
	return
}

// Scan all items in tree
func (tr *BTree) Scan(iter func(key int64, value interface{}) bool) {
	if tr.root != nil {
		tr.root.scan(iter, tr.height)
	}
}

func (n *node) scan(
	iter func(key int64, value interface{}) bool, height int,
) bool {
	if height == 0 {
		for i := 0; i < n.numItems; i++ {
			if !iter(n.items[i].key, n.items[i].value) {
				return false
			}
		}
		return true
	}
	for i := 0; i < n.numItems; i++ {
		if !n.children[i].scan(iter, height-1) {
			return false
		}
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
	}
	return n.children[n.numItems].scan(iter, height-1)
}

// Get a value for key
func (tr *BTree) Get(key int64) (value interface{}, gotten bool) {
	if tr.root == nil {
		return
	}
	return tr.root.get(key, tr.height)
}

func (n *node) get(key int64, height int) (value interface{}, gotten bool) {
	i, found := n.find(key)
	if found {
		return n.items[i].value, true
	}
	if height == 0 {
		return nil, false
	}
	return n.children[i].get(key, height-1)
}

// Len returns the number of items in the tree
func (tr *BTree) Len() int {
	return tr.length
}

// Delete a value for a key
func (tr *BTree) Delete(key int64) (prev interface{}, deleted bool) {
	if tr.root == nil {
		return
	}
	var prevItem item
	prevItem, deleted = tr.root.delete(false, key, tr.height)
	if !deleted {
		return
	}
	prev = prevItem.value
	if tr.root.numItems == 0 {
		tr.root = tr.root.children[0]
		tr.height--
	}
	tr.length--
	if tr.length == 0 {
		tr.root = nil
		tr.height = 0
	}
	return
}

func (n *node) delete(max bool, key int64, height int) (
	prev item, deleted bool,
) {
	i, found := 0, false
	if max {
		i, found = n.numItems-1, true
	} else {
		i, found = n.find(key)
	}
	if height == 0 {
		if found {
			prev = n.items[i]
			// found the items at the leaf, remove it and return.
			copy(n.items[i:], n.items[i+1:n.numItems])
			n.items[n.numItems-1] = item{}
			n.children[n.numItems] = nil
			n.numItems--
			return prev, true
		}
		return item{}, false
	}

	if found {
		if max {
			i++
			prev, deleted = n.children[i].delete(true, freeKey, height-1)
		} else {
			prev = n.items[i]
			maxItem, _ := n.children[i].delete(true, freeKey, height-1)
			n.items[i] = maxItem
			deleted = true
		}
	} else {
		prev, deleted = n.children[i].delete(max, key, height-1)
	}
	if !deleted {
		return
	}
	if n.children[i].numItems < minItems {
		if i == n.numItems {
			i--
		}
		if n.children[i].numItems+n.children[i+1].numItems+1 < maxItems {
			// merge left + item + right
			n.children[i].items[n.children[i].numItems] = n.items[i]
			copy(n.children[i].items[n.children[i].numItems+1:],
				n.children[i+1].items[:n.children[i+1].numItems])
			if height > 1 {
				copy(n.children[i].children[n.children[i].numItems+1:],
					n.children[i+1].children[:n.children[i+1].numItems+1])
			}
			n.children[i].numItems += n.children[i+1].numItems + 1
			copy(n.items[i:], n.items[i+1:n.numItems])
			copy(n.children[i+1:], n.children[i+2:n.numItems+1])
			n.items[n.numItems] = item{}
			n.children[n.numItems+1] = nil
			n.numItems--
		} else if n.children[i].numItems > n.children[i+1].numItems {
			// move left -> right
			copy(n.children[i+1].items[1:],
				n.children[i+1].items[:n.children[i+1].numItems])
			if height > 1 {
				copy(n.children[i+1].children[1:],
					n.children[i+1].children[:n.children[i+1].numItems+1])
			}
			n.children[i+1].items[0] = n.items[i]
			if height > 1 {
				n.children[i+1].children[0] =
					n.children[i].children[n.children[i].numItems]
			}
			n.children[i+1].numItems++
			n.items[i] = n.children[i].items[n.children[i].numItems-1]
			n.children[i].items[n.children[i].numItems-1] = item{}
			if height > 1 {
				n.children[i].children[n.children[i].numItems] = nil
			}
			n.children[i].numItems--
		} else {
			// move right -> left
			n.children[i].items[n.children[i].numItems] = n.items[i]
			if height > 1 {
				n.children[i].children[n.children[i].numItems+1] =
					n.children[i+1].children[0]
			}
			n.children[i].numItems++
			n.items[i] = n.children[i+1].items[0]
			copy(n.children[i+1].items[:],
				n.children[i+1].items[1:n.children[i+1].numItems])
			if height > 1 {
				copy(n.children[i+1].children[:],
					n.children[i+1].children[1:n.children[i+1].numItems+1])
			}
			n.children[i+1].numItems--
		}
	}
	return
}

// Ascend the tree within the range [pivot, last]
func (tr *BTree) Ascend(
	pivot int64,
	iter func(key int64, value interface{}) bool,
) {
	if tr.root != nil {
		tr.root.ascend(pivot, iter, tr.height)
	}
}

func (n *node) ascend(
	pivot int64,
	iter func(key int64, value interface{}) bool,
	height int,
) bool {
	i, found := n.find(pivot)
	if !found {
		if height > 0 {
			if !n.children[i].ascend(pivot, iter, height-1) {
				return false
			}
		}
	}
	for ; i < n.numItems; i++ {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if height > 0 {
			if !n.children[i+1].scan(iter, height-1) {
				return false
			}
		}
	}
	return true
}

// Reverse all items in tree
func (tr *BTree) Reverse(iter func(key int64, value interface{}) bool) {
	if tr.root != nil {
		tr.root.reverse(iter, tr.height)
	}
}

func (n *node) reverse(
	iter func(key int64, value interface{}) bool, height int,
) bool {
	if height == 0 {
		for i := n.numItems - 1; i >= 0; i-- {
			if !iter(n.items[i].key, n.items[i].value) {
				return false
			}
		}
		return true
	}
	if !n.children[n.numItems].reverse(iter, height-1) {
		return false
	}
	for i := n.numItems - 1; i >= 0; i-- {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if !n.children[i].reverse(iter, height-1) {
			return false
		}
	}
	return true
}

// Descend the tree within the range [pivot, first]
func (tr *BTree) Descend(
	pivot int64,
	iter func(key int64, value interface{}) bool,
) {
	if tr.root != nil {
		tr.root.descend(pivot, iter, tr.height)
	}
}

func (n *node) descend(
	pivot int64,
	iter func(key int64, value interface{}) bool,
	height int,
) bool {
	i, found := n.find(pivot)
	if !found {
		if height > 0 {
			if !n.children[i].descend(pivot, iter, height-1) {
				return false
			}
		}
		i--
	}
	for ; i >= 0; i-- {
		if !iter(n.items[i].key, n.items[i].value) {
			return false
		}
		if height > 0 {
			if !n.children[i].reverse(iter, height-1) {
				return false
			}
		}
	}
	return true
}

func (tr *BTree) GreaterOrEqual(
	pivot int64,
	iter func(key int64, value interface{}) bool,
) {
	if tr.root != nil {
		tr.root.ascend(pivot, iter, tr.height)
	}
}

func (tr *BTree) LessOrEqual(
	pivot int64,
	iter func(key int64, value interface{}) bool,
) {
	if tr.root != nil {
		tr.root.descend(pivot, iter, tr.height)
	}
}

func (tr *BTree) Next(pivot int64) (key int64, value interface{}) {
	i := 0
	tr.GreaterOrEqual(pivot, func(k int64, v interface{}) bool {
		if i == 1 {
			key = k
			value = v
			return false
		}
		i++
		return true
	})
	return
}

func (tr *BTree) Prev(pivot int64) (key int64, value interface{}) {
	i := 0
	tr.LessOrEqual(pivot, func(k int64, v interface{}) bool {
		if i == 1 {
			key = k
			value = v
			return false
		}
		i++
		return true
	})
	return
}

func (tr *BTree) GetOrNearest(key int64) (nKey int64, nValue interface{}) {
	if tr.root == nil {
		return
	}
	return tr.root.getOrNearest(key, tr.height)
}

func (n *node) getOrNearest(key int64, height int) (nKey int64, nValue interface{}) {
	i, found := n.find(key)
	if found {
		return n.items[i].key, n.items[i].value
	}

	if height == 0 {
		//fmt.Printf("index: %d, items: %v\n", i, n.items)
		if i > 0 {
			return n.items[i-1].key, n.items[i-1].value
		}
	}

	// если дошли до последней родительской ноды, то:
	// 1. проверим, есть ли элемент в последней дочерней ноде, если есть, то вернем его
	// 2. если в дочерней ноде элемента нет, но c.find вернул ci > 0, это значит что ближайший меньший элемент все таки находится в этой дочерней ноде и равен c.items[ci-1]
	// 3. иначе ближайший меньший элемент находится в родительской ноде и равен n.items[i-1]
	if height == 1 {
		c := n.children[i]
		ci, found := c.find(key)
		if found {
			return c.items[ci].key, c.items[ci].value
		}

		//fmt.Printf("child index: %d, child items: %v\n", ci, c.items)
		if ci > 0 {
			return c.items[ci-1].key, c.items[ci-1].value
		}

		//fmt.Printf("index: %d, items: %v\n", i, n.items)
		if i > 0 {
			return n.items[i-1].key, n.items[i-1].value
		}

		return
	}

	return n.children[i].getOrNearest(key, height-1)
}
