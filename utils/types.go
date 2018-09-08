package utils

const (
	STRING_TYPE  byte = 0
	SET_TYPE     byte = 1
	SET_DATA     byte = 2
	ZSET_TYPE    byte = 3
	ZSET_DATA    byte = 4
	ZSET_SCORE   byte = 5
	HASH_TYPE    byte = 6
	HASH_DATA    byte = 7
	LIST_TYPE    byte = 8
	LIST_DATA    byte = 9
	TTL_TYPE     byte = 109
	EXPTIME_TYPE byte = 110
)
const (
	FLAG_NORMAL byte = iota
	FLAG_DELETED
)

const (
	LHeadDirection    uint8  = 0
	LTailDirection    uint8  = 1
	LItemDefaultIndex uint64 = 1<<32 - 512
)

var (
	EmptyListInterfaces []interface{} = make([]interface{}, 0)
)

func ChkPrefix(src []byte) bool {
	if len(src) == 0 {
		return false
	}
	switch src[0] {
	case '-':
		return true
	case '+':
		return true
	case '(':
		return true
	case '[':
		return true
	default:
		return false
	}
}
