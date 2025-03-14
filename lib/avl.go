package main

import "strconv"

const (
	SSTABLE_DIRECTORY = "./sstable"
	LOG_FILE_NAME     = "log.txt"
	SEGMENT_SIZE      = 10 * 1024
	MAX_TREE_SIZE     = 16 * 1024
)

var (
	fileNameAndSSTableMap = make(map[string]*SSTable)
	avlTree               = NewAVL()
	writeAheadLog         *WriteAheadLog
	compaction            *Compaction
	currentTreeSize       int
)

type SSTable struct {
	sparseIndex *SparseIndex
	bloomFilter *BloomFilter
	byteOffset  int64
	fileName    string
}

func NewSSTable() *SSTable {
	return &SSTable{
		sparseIndex: NewSparseIndex(),
		bloomFilter: NewBloomFilter(),
		byteOffset:  0,
		fileName:    "sstable" + strconv.Itoa(getSSTableCountPlusOne()) + ".txt",
	}
}
