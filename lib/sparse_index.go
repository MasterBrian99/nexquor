package main

type SparseIndex struct {
	fileName string
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{index: make(map[string]int64)}
}
