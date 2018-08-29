package tidis

import (
	"github.com/chuangyou/qkv/config"
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/store"
	"github.com/pingcap/tidb/kv"
)

type Tidis struct {
	conf *config.Config
	db   store.DB
}

func NewTidis(conf *config.Config) (*Tidis, error) {
	tidis := new(Tidis)
	db, err := store.Open(conf)
	if err != nil {
		return nil, err
	}
	tidis.conf = conf
	tidis.db = db
	return tidis, nil
}
func (tidis *Tidis) NewTxn() (tikvTxn kv.Transaction, err error) {
	var (
		txn interface{}
		ok  bool
	)
	txn, err = tidis.db.NewTxn()
	tikvTxn, ok = txn.(kv.Transaction)
	if !ok {
		err = qkverror.ErrorServerInternal
		return
	}
	return
}
