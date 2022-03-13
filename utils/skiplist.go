package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
	p               = 0.5
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	return &SkipList{
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		maxLevel: defaultMaxLevel,
		length:   0,
		header:   newElement(0, nil, defaultMaxLevel),
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//implement me here!!!
	prevElem := list.header

	var prevHeaderListElem [defaultMaxLevel]*Element

	i := len(prevElem.levels) - 1

	score := list.calcScore(data.Key)

	for i >= 0 {
		prevHeaderListElem[i] = prevElem

	L:
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			switch list.compare(score, data.Key, next) {
			case 0:
				next.entry = data
				return nil
			case -1:
				break L
			}
			prevElem = next
			prevHeaderListElem[i] = prevElem
		}
		i--
	}

	level := list.randLevel()
	elem := newElement(score, data, level)

	for i := 0; i < level; i++ {
		elem.levels[i] = prevHeaderListElem[i].levels[i]
		prevHeaderListElem[i].levels[i] = elem
	}

	list.size += data.Size()
	list.length++

	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!

	if list.length == 0 {
		return nil
	}

	var prevElem *Element = list.header

	i := len(list.header.levels) - 1

	for i >= 0 {

		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
		L:
			switch list.compare(list.calcScore(key), key, next) {
			case 0:
				return next.entry
			case -1:
				break L
			}
			prevElem = next
		}

		i--
	}

	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score > next.score {
		return 1
	}
	return -1

}

func (list *SkipList) randLevel() int {
	//implement me here!!!

	var level int = 1
	for Float64() < p {
		level++
	}
	if level > defaultMaxLevel {
		level = defaultMaxLevel
	}
	return level
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
