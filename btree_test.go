package tinybtree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	seed := time.Now().UnixNano()
	fmt.Printf("seed: %d\n", seed)
	rand.Seed(seed)
}

func randKeys(N int) (keys []int) {
	for _, i := range rand.Perm(N) {
		keys = append(keys, i)
	}
	return
}

const flatLeaf = true

func (tr *BTree) print() {
	tr.root.print(0, tr.height)
}

func (n *node) print(level, height int) {
	if n == nil {
		println("NIL")
		return
	}
	if height == 0 && flatLeaf {
		fmt.Printf("%s", strings.Repeat("  ", level))
	}
	for i := 0; i < n.numItems; i++ {
		if height > 0 {
			n.children[i].print(level+1, height-1)
		}
		if height > 0 || (height == 0 && !flatLeaf) {
			fmt.Printf("%s%v\n", strings.Repeat("  ", level), n.items[i].key)
		} else {
			if i > 0 {
				fmt.Printf(",")
			}
			fmt.Printf("%d", n.items[i].key)
		}
	}
	if height == 0 && flatLeaf {
		fmt.Printf("\n")
	}
	if height > 0 {
		n.children[n.numItems].print(level+1, height-1)
	}
}

func (tr *BTree) deepPrint() {
	fmt.Printf("%#v\n", tr)
	tr.root.deepPrint(0, tr.height)
}

func (n *node) deepPrint(level, height int) {
	if n == nil {
		fmt.Printf("%s %#v\n", strings.Repeat("  ", level), n)
		return
	}
	fmt.Printf("%s count: %v\n", strings.Repeat("  ", level), n.numItems)
	fmt.Printf("%s items: %v\n", strings.Repeat("  ", level), n.items)
	if height > 0 {
		fmt.Printf("%s child: %v\n", strings.Repeat("  ", level), n.children)
	}
	if height > 0 {
		for i := 0; i < n.numItems; i++ {
			n.children[i].deepPrint(level+1, height-1)
		}
		n.children[n.numItems].deepPrint(level+1, height-1)
	}
}

func intsEquals(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDescend(t *testing.T) {
	var tr BTree
	var count int
	tr.Descend(1, func(key int64, value interface{}) bool {
		count++
		return true
	})
	if count > 0 {
		t.Fatalf("expected 0, got %v", count)
	}
	var keys []int64
	for i := int64(0); i < 1000; i += 10 {
		keys = append(keys, i)
		tr.Set(keys[len(keys)-1], nil)
	}
	var exp []int64
	tr.Reverse(func(key int64, _ interface{}) bool {
		exp = append(exp, key)
		return true
	})
	for i := int64(999); i >= 0; i-- {
		var all []int64
		tr.Descend(i, func(key int64, value interface{}) bool {
			all = append(all, key)
			return true
		})
		for len(exp) > 0 && i < exp[0] {
			exp = exp[1:]
		}
		var count int64
		tr.Descend(i, func(key int64, value interface{}) bool {
			if count == (i+1)%maxItems {
				return false
			}
			count++
			return true
		})
		if int(count) > len(exp) {
			t.Fatalf("expected 1, got %v", count)
		}

		if !intsEquals(exp, all) {
			fmt.Printf("exp: %v\n", exp)
			fmt.Printf("all: %v\n", all)
			t.Fatal("mismatch")
		}
	}
}

func TestAscend(t *testing.T) {
	var tr BTree
	var count int
	tr.Ascend(1, func(key int64, value interface{}) bool {
		count++
		return true
	})
	if count > 0 {
		t.Fatalf("expected 0, got %v", count)
	}
	var keys []int64
	for i := int64(0); i < 1000; i += 10 {
		keys = append(keys, i)
		tr.Set(keys[len(keys)-1], nil)
	}
	exp := keys
	for i := int64(-1); i < 1000; i++ {
		var all []int64
		tr.Ascend(i, func(key int64, value interface{}) bool {
			all = append(all, key)
			return true
		})

		for len(exp) > 0 && i > exp[0] {
			exp = exp[1:]
		}
		var count int64
		tr.Ascend(i, func(key int64, value interface{}) bool {
			if count == (i+1)%maxItems {
				return false
			}
			count++
			return true
		})
		if int(count) > len(exp) {
			t.Fatalf("expected 1, got %v", count)
		}
		if !intsEquals(exp, all) {
			t.Fatal("mismatch")
		}
	}
}

func TestBTree(t *testing.T) {
	N := 10000
	var tr BTree
	keys := randKeys(N)

	// insert all items
	for _, key := range keys {
		value, replaced := tr.Set(int64(key), key)
		if replaced {
			t.Fatal("expected false")
		}
		if value != nil {
			t.Fatal("expected nil")
		}
	}

	// check length
	if tr.Len() != len(keys) {
		t.Fatalf("expected %v, got %v", len(keys), tr.Len())
	}

	// get each value
	for _, key := range keys {
		value, gotten := tr.Get(int64(key))
		if !gotten {
			t.Fatal("expected true")
		}
		if value == nil || value.(int) != key {
			t.Fatalf("expected '%v', got '%v'", key, value)
		}
	}

	// scan all items
	var last int64 = -1
	all := make(map[int64]interface{})
	tr.Scan(func(key int64, value interface{}) bool {
		if key <= last {
			t.Fatal("out of order", key, last)
		}
		if value.(int) != int(key) {
			t.Fatalf("mismatch")
		}
		last = key
		all[key] = value
		return true
	})
	if len(all) != len(keys) {
		t.Fatalf("expected '%v', got '%v'", len(keys), len(all))
	}

	// reverse all items
	var prev int64 = -1
	all = make(map[int64]interface{})
	tr.Reverse(func(key int64, value interface{}) bool {
		if prev != -1 && key >= prev {
			t.Fatal("out of order", key, prev)
		}
		if value.(int) != int(key) {
			t.Fatalf("mismatch")
		}
		prev = key
		all[key] = value
		return true
	})
	if len(all) != len(keys) {
		t.Fatalf("expected '%v', got '%v'", len(keys), len(all))
	}

	// try to get an invalid item
	value, gotten := tr.Get(-100)
	if gotten {
		t.Fatal("expected false")
	}
	if value != nil {
		t.Fatal("expected nil")
	}

	// scan and quit at various steps
	for i := int64(0); i < 100; i++ {
		var j int64
		tr.Scan(func(key int64, value interface{}) bool {
			if j == i {
				return false
			}
			j++
			return true
		})
	}

	// reverse and quit at various steps
	for i := int64(0); i < 100; i++ {
		var j int64
		tr.Reverse(func(key int64, value interface{}) bool {
			if j == i {
				return false
			}
			j++
			return true
		})
	}

	// delete half the items
	for _, key := range keys[:len(keys)/2] {
		value, deleted := tr.Delete(int64(key))
		if !deleted {
			t.Fatal("expected true")
		}
		if value == nil || value.(int) != key {
			t.Fatalf("expected '%v', got '%v'", key, value)
		}
	}

	// check length
	if tr.Len() != len(keys)/2 {
		t.Fatalf("expected %v, got %v", len(keys)/2, tr.Len())
	}

	// try delete half again
	for _, key := range keys[:len(keys)/2] {
		value, deleted := tr.Delete(int64(key))
		if deleted {
			t.Fatal("expected false")
		}
		if value != nil {
			t.Fatalf("expected nil")
		}
	}

	// try delete half again
	for _, key := range keys[:len(keys)/2] {
		value, deleted := tr.Delete(int64(key))
		if deleted {
			t.Fatal("expected false")
		}
		if value != nil {
			t.Fatalf("expected nil")
		}
	}

	// check length
	if tr.Len() != len(keys)/2 {
		t.Fatalf("expected %v, got %v", len(keys)/2, tr.Len())
	}

	// scan items
	last = -1
	all = make(map[int64]interface{})
	tr.Scan(func(key int64, value interface{}) bool {
		if key <= last {
			t.Fatal("out of order")
		}
		if value.(int) != int(key) {
			t.Fatalf("mismatch")
		}
		last = key
		all[key] = value
		return true
	})
	if len(all) != len(keys)/2 {
		t.Fatalf("expected '%v', got '%v'", len(keys), len(all))
	}

	// replace second half
	for _, key := range keys[len(keys)/2:] {
		value, replaced := tr.Set(int64(key), key)
		if !replaced {
			t.Fatal("expected true")
		}
		if value == nil || value.(int) != key {
			t.Fatalf("expected '%v', got '%v'", key, value)
		}
	}

	// delete next half the items
	for _, key := range keys[len(keys)/2:] {
		value, deleted := tr.Delete(int64(key))
		if !deleted {
			t.Fatal("expected true")
		}
		if value == nil || value.(int) != key {
			t.Fatalf("expected '%v', got '%v'", key, value)
		}
	}

	// check length
	if tr.Len() != 0 {
		t.Fatalf("expected %v, got %v", 0, tr.Len())
	}

	// do some stuff on an empty tree
	value, gotten = tr.Get(int64(keys[0]))
	if gotten {
		t.Fatal("expected false")
	}
	if value != nil {
		t.Fatal("expected nil")
	}
	tr.Scan(func(key int64, value interface{}) bool {
		t.Fatal("should not be reached")
		return true
	})
	tr.Reverse(func(key int64, value interface{}) bool {
		t.Fatal("should not be reached")
		return true
	})

	var deleted bool
	value, deleted = tr.Delete(-100)
	if deleted {
		t.Fatal("expected false")
	}
	if value != nil {
		t.Fatal("expected nil")
	}
}

func BenchmarkTidwallSequentialSet(b *testing.B) {
	var tr BTree
	keys := randKeys(b.N)
	sort.Ints(keys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.Set(int64(keys[i]), nil)
	}
}

func BenchmarkTidwallSequentialGet(b *testing.B) {
	var tr BTree
	keys := randKeys(b.N)
	sort.Ints(keys)
	for i := 0; i < b.N; i++ {
		tr.Set(int64(keys[i]), nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.Get(int64(keys[i]))
	}
}

func BenchmarkTidwallRandomSet(b *testing.B) {
	var tr BTree
	keys := randKeys(b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.Set(int64(keys[i]), nil)
	}
}

func BenchmarkTidwallRandomGet(b *testing.B) {
	var tr BTree
	keys := randKeys(b.N)
	for i := 0; i < b.N; i++ {
		tr.Set(int64(keys[i]), nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr.Get(int64(keys[i]))
	}
}

// type googleKind struct {
// 	key string
// }

// func (a *googleKind) Less(b btree.Item) bool {
// 	return a.key < b.(*googleKind).key
// }

// func BenchmarkGoogleSequentialSet(b *testing.B) {
// 	tr := btree.New(32)
// 	keys := randKeys(b.N)
// 	sort.Strings(keys)
// 	gkeys := make([]*googleKind, len(keys))
// 	for i := 0; i < b.N; i++ {
// 		gkeys[i] = &googleKind{keys[i]}
// 	}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		tr.ReplaceOrInsert(gkeys[i])
// 	}
// }

// func BenchmarkGoogleSequentialGet(b *testing.B) {
// 	tr := btree.New(32)
// 	keys := randKeys(b.N)
// 	gkeys := make([]*googleKind, len(keys))
// 	for i := 0; i < b.N; i++ {
// 		gkeys[i] = &googleKind{keys[i]}
// 	}
// 	for i := 0; i < b.N; i++ {
// 		tr.ReplaceOrInsert(gkeys[i])
// 	}
// 	sort.Strings(keys)
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		tr.Get(gkeys[i])
// 	}
// }

// func BenchmarkGoogleRandomSet(b *testing.B) {
// 	tr := btree.New(32)
// 	keys := randKeys(b.N)
// 	gkeys := make([]*googleKind, len(keys))
// 	for i := 0; i < b.N; i++ {
// 		gkeys[i] = &googleKind{keys[i]}
// 	}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		tr.ReplaceOrInsert(gkeys[i])
// 	}
// }

// func BenchmarkGoogleRandomGet(b *testing.B) {
// 	tr := btree.New(32)
// 	keys := randKeys(b.N)
// 	gkeys := make([]*googleKind, len(keys))
// 	for i := 0; i < b.N; i++ {
// 		gkeys[i] = &googleKind{keys[i]}
// 	}
// 	for i := 0; i < b.N; i++ {
// 		tr.ReplaceOrInsert(gkeys[i])
// 	}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		tr.Get(gkeys[i])
// 	}
// }

func TestBTreeOne(t *testing.T) {
	var tr BTree
	tr.Set(1, "1")
	tr.Delete(1)
	tr.Set(1, "1")
	tr.Delete(1)
	tr.Set(1, "1")
	tr.Delete(1)
}

func TestBTree256(t *testing.T) {
	var tr BTree
	var n int
	for j := 0; j < 2; j++ {
		for _, i := range rand.Perm(256) {
			tr.Set(int64(i), i)
			n++
			if tr.Len() != n {
				t.Fatalf("expected 256, got %d", n)
			}
		}
		for _, i := range rand.Perm(256) {
			v, ok := tr.Get(int64(i))
			if !ok {
				t.Fatal("expected true")
			}
			if v.(int) != int(i) {
				t.Fatalf("expected %d, got %d", i, v.(int64))
			}
		}
		for _, i := range rand.Perm(256) {
			tr.Delete(int64(i))
			n--
			if tr.Len() != n {
				t.Fatalf("expected 256, got %d", n)
			}
		}
		for _, i := range rand.Perm(256) {
			_, ok := tr.Get(int64(i))
			if ok {
				t.Fatal("expected false")
			}
		}
	}
}

func TestBTreeRandom(t *testing.T) {
	var count uint32
	T := runtime.NumCPU()
	D := time.Second
	N := 1000
	bkeys := make([]int, N)
	for i, key := range rand.Perm(N) {
		bkeys[i] = key
	}

	var wg sync.WaitGroup
	wg.Add(T)
	for i := 0; i < T; i++ {
		go func() {
			defer wg.Done()
			start := time.Now()
			for {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				keys := make([]int, len(bkeys))
				for i, key := range bkeys {
					keys[i] = key
				}
				testBTreeRandom(t, r, keys, &count)
				if time.Since(start) > D {
					break
				}
			}
		}()
	}
	wg.Wait()
}

func shuffle(r *rand.Rand, keys []int) {
	for i := range keys {
		j := r.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}
}

func testBTreeRandom(t *testing.T, r *rand.Rand, keys []int, count *uint32) {
	var tr BTree
	keys = keys[:rand.Intn(len(keys))]
	shuffle(r, keys)
	for i := 0; i < len(keys); i++ {
		prev, ok := tr.Set(int64(keys[i]), keys[i])
		if ok || prev != nil {
			t.Fatalf("expected nil")
		}
	}
	shuffle(r, keys)
	for i := 0; i < len(keys); i++ {
		prev, ok := tr.Get(int64(keys[i]))
		if !ok || prev != keys[i] {
			t.Fatalf("expected '%v', got '%v'", keys[i], prev)
		}
	}
	shuffle(r, keys)
	for i := 0; i < len(keys); i++ {
		prev, ok := tr.Delete(int64(keys[i]))
		if !ok || prev != keys[i] {
			t.Fatalf("expected '%v', got '%v'", keys[i], prev)
		}
		prev, ok = tr.Get(int64(keys[i]))
		if ok || prev != nil {
			t.Fatalf("expected nil")
		}
	}
	atomic.AddUint32(count, 1)
}

func TestBTreeScan(t *testing.T) {

	const c = 1000
	s := make([]int, c)
	e := make([]int, c)
	for i := 0; i < c; i++ {
		s[i] = rand.Int()
	}
	copy(e, s)
	sort.Ints(e)

	var tree BTree
	for i := 0; i < c; i++ {
		tree.Set(int64(s[i]), "x")
	}
	assert.Equal(t, c, tree.Len())

	// убеждаемся что полный проход по дереву дает нам отсортированный массив
	a := make([]int, 0, c)
	tree.Scan(func(key int64, value interface{}) bool {
		a = append(a, int(key))
		return true
	})
	assert.Equal(t, e, a)

	// находим какой-нибудь элемент около середины дерева
	r := rand.Intn(10)
	i := (c/2 - 5) + r

	// выбираем все элементы, которые больше
	a = make([]int, 0, c)
	tree.GreaterOrEqual(int64(e[i]), func(key int64, value interface{}) bool {
		a = append(a, int(key))
		return true
	})
	assert.Equal(t, e[i:], a)

	// выбираем все элементы, которые меньше
	a = make([]int, 0, c)
	tree.LessOrEqual(int64(e[i]), func(key int64, value interface{}) bool {
		a = append(a, int(key))
		return true
	})
	er := e[:i+1]
	for left, right := 0, len(er)-1; left < right; left, right = left+1, right-1 {
		er[left], er[right] = er[right], er[left]
	}
	assert.Equal(t, er, a)
}

func TestBTreeNearest(t *testing.T) {

	var tree BTree
	for i := int64(0); i < 200; i += 2 {
		tree.Set(i, i)
	}

	type args struct {
		key  int64
		near int64
	}
	tests := []args{
		{0, 0}, {1, 0}, {2, 2}, {3, 2},
		{61, 60}, {62, 62}, {63, 62},
		{197, 196}, {199, 198}, {199, 198}, {202, 198},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			k, _ := tree.GetOrNearest(tt.key)
			assert.Equal(t, tt.near, k)
		})
	}
}

func TestBTreeNearest2(t *testing.T) {

	var tree BTree
	tree.Set(1, "x")
	tree.Set(10, "x")
	tree.Set(20, "x")
	tree.Set(30, "x")
	tree.Set(40, "x")
	tree.Set(50, "x")

	key, value := tree.GetOrNearest(25)
	fmt.Printf("near: %v = %v\n", key, value)

	nKey, nValue := tree.Next(key)
	fmt.Printf("next: %v = %v\n", nKey, nValue)

	pKey, pValue := tree.Prev(key)
	fmt.Printf("prev: %v = %v\n", pKey, pValue)
}

func TestBTreePrev(t *testing.T) {
	var tree BTree
	for i := int64(1); i <= 10000000; i++ {
		tree.Set(i, "x")
	}
	assert.Equal(t, 10000000, tree.Len())

	var i int64 = 5000000
	key, _ := tree.GetOrNearest(i)
	tree.LessOrEqual(key, func(k int64, v interface{}) bool {
		if k != i {
			t.Fatalf("mismatch, key: %d, i: %d", k, i)
		}
		i--
		return true
	})
}

func TestBTreeNext(t *testing.T) {
	var tree BTree
	for i := int64(1); i <= 10000000; i++ {
		tree.Set(i, "x")
	}
	assert.Equal(t, 10000000, tree.Len())

	var i int64 = 5000000
	key, _ := tree.GetOrNearest(i)
	tree.GreaterOrEqual(key, func(k int64, v interface{}) bool {
		if k != i {
			t.Fatalf("mismatch, key: %d, i: %d", k, i)
		}
		i++
		return true
	})
}

func BenchmarkBTreePut(b *testing.B) {

	var tree BTree
	for i := int64(1); i <= 10000000; i++ {
		tree.Set(i*10000000, "x")
	}
	assert.Equal(b, 10000000, tree.Len())

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		tree.Set(int64(n), "x")
	}
}

func BenchmarkBTreeGet(b *testing.B) {

	var tree BTree
	for i := 1; i <= 10000000; i++ {
		tree.Set(int64(i), "x")
	}
	assert.Equal(b, 10000000, tree.Len())

	b.ReportAllocs()
	b.ResetTimer()

	for n := 1; n < b.N; n++ {
		_, _ = tree.Get(int64(n))
	}
}

func BenchmarkBTreeRemove(b *testing.B) {

	var tree BTree
	for i := 1; i <= 10000000; i++ {
		tree.Set(int64(i), "x")
	}
	assert.Equal(b, 10000000, tree.Len())

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		tree.Delete(int64(n))
	}
}

func BenchmarkBTreeIterate(b *testing.B) {

	var tree BTree
	for i := 1; i <= 10000000; i++ {
		tree.Set(int64(i*10000000), i)
	}
	assert.Equal(b, 10000000, tree.Len())

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		tree.Scan(func(key int64, value interface{}) bool {
			return true
		})
	}
}
