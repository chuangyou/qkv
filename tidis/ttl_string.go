package tidis

import (
	"context"
	"math"
	"time"

	ti "github.com/chuangyou/qkv/store/tikv"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
	log "github.com/sirupsen/logrus"
)

func StringTTLCheckerRun(maxLoops, interval int, tdb *Tidis) {
	var (
		c        <-chan time.Time
		startKey []byte
		endKey   []byte
		err      error
		ret      int
		tikv_txn kv.Transaction
	)
	c = time.Tick(time.Duration(interval) * time.Millisecond)
	for _ = range c {
		startKey = utils.EncodeExpireKey([]byte{0}, utils.STRING_TYPE, 0)
		endKey = utils.EncodeExpireKey([]byte{0}, utils.STRING_TYPE, math.MaxInt64)
		tikv_txn, err = tdb.NewTxn()
		if err != nil {
			log.Warnf("ttl checker start transation failed, %s", err.Error())
			continue
		}
		ret, err = delExpStringType(tdb, tikv_txn, startKey, endKey, maxLoops)
		if err != nil {
			log.Warnf("string ttl checker decode key failed, %s", err.Error())
		} else {
			if ret == -1 {
				//log.Debugf("string ttl checker execute none")
			} else {
				log.Debugf("string ttl checker execute %d keys", ret)
			}
		}
	}
}
func delExpStringType(tdb *Tidis, tikv_txn kv.Transaction, startKey, endKey []byte, maxLoops int) (ret int, err error) {
	var (
		loops    int
		snapshot kv.Snapshot
		it       *ti.Iterator
		key      []byte
		ts       uint64
		ttlKey   []byte
		dataKey  []byte
	)
	defer tikv_txn.Rollback()
	snapshot = tikv_txn.GetSnapshot()
	it, err = ti.NewIterator(startKey, endKey, snapshot, false)
	if err != nil {
		return
	}
	defer it.Close()
	loops = maxLoops
	for loops > 0 && it.Valid() {
		key, ts, err = utils.DecodeExpireKey(it.Key(), utils.STRING_TYPE)
		if err != nil {
			return
		}
		if ts > uint64(time.Now().UnixNano()/1000/1000) {
			// no key expired
			break
		}
		ttlKey = utils.EncodeTTLKey(key, utils.STRING_TYPE)
		dataKey = utils.EncodeStringKey(key)
		if err = tikv_txn.Delete(it.Key()); err != nil {
			return
		}
		if err = tikv_txn.Delete(ttlKey); err != nil {
			return
		}
		if err = tikv_txn.Delete(dataKey); err != nil {
			return
		}
		it.Next()
		loops--
	}
	err = tikv_txn.Commit(context.Background())
	if maxLoops == loops {
		//no action
		ret = -1
	} else {
		ret = maxLoops - loops
	}
	return
}
