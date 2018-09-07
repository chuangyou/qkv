package store

type DB interface {
	Close() error
	Get(interface{}, []byte) ([]byte, error)
	Set(interface{}, []byte, []byte) error
	MGet(interface{}, [][]byte) (map[string][]byte, error)
	MSet(interface{}, map[string][]byte) (int, error)
	DeleteWithTxn(interface{}, [][]byte) (int64, error)
	SetEX(interface{}, []byte, int64, []byte) error
	PExipre(interface{}, []byte, int64) (int, error)
	NewTxn() (interface{}, error)
}
