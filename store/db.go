package store

type DB interface {
	Close() error
	Get(interface{}, []byte) ([]byte, error)
	Set(interface{}, []byte, []byte) error
	MGet(interface{}, [][]byte) (map[string][]byte, error)
	MSet(interface{}, map[string][]byte) (int, error)
	DeleteWithTxn(interface{}, [][]byte) (int64, error)
	ClearExpire(interface{}, []byte) error
	TTL(interface{}, []byte) (int64, error)
	PTTL(interface{}, []byte) (int64, error)
	SetEX(interface{}, []byte, int64, []byte) error
	PExipre(interface{}, []byte, int64) (int, error)
	Incr(interface{}, []byte, int64) (int64, error)
	DeleteRangeWithTxn(interface{}, []byte, []byte, uint64) (uint64, error)
	GetRangeKeys(interface{}, []byte, bool, []byte, bool, uint64, uint64, bool) ([][]byte, uint64, error)
	NewTxn() (interface{}, error)
}
