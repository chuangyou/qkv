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

func TTLCheckerRun(tdb *Tidis, maxLoops, interval int) {
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
		startKey = utils.EncodeExpireKey([]byte{0}, 0)
		endKey = utils.EncodeExpireKey([]byte{0}, math.MaxInt64)
		tikv_txn, err = tdb.NewTxn()
		if err != nil {
			log.Warnf("ttl checker start transation failed, %s", err.Error())
			continue
		}
		ret, err = delExpireKey(tdb, tikv_txn, startKey, endKey, maxLoops)
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
func delExpireKey(tdb *Tidis, tikv_txn kv.Transaction, startKey, endKey []byte, maxLoops int) (ret int, err error) {
	var (
		loops    int
		snapshot kv.Snapshot
		it       *ti.Iterator
		key      []byte
		ts       uint64
		ttlKey   []byte
		rawData  []byte
		dataType byte
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
		key, ts, err = utils.DecodeExpireKey(it.Key())
		if err != nil {
			return
		}
		if ts > uint64(time.Now().UnixNano()/1000/1000) {
			// no key expired
			break
		}
		ttlKey = utils.EncodeTTLKey(key)
		//delete expire key
		if err = tikv_txn.Delete(it.Key()); err != nil {
			return
		}
		//delete ttl key
		if err = tikv_txn.Delete(ttlKey); err != nil {
			return
		}
		rawData, err = tdb.db.Get(tikv_txn, key)
		if err != nil {
			return
		}
		if rawData != nil {
			dataType, _, err = utils.DecodeData(rawData)
			if err != nil {
				return
			}
		}
		switch dataType {
		//delete set member
		case utils.SET_TYPE:
			if _, err = tdb.ClearSetMembers(tikv_txn, key); err != nil {
				return
			}
		//delete zset member
		case utils.ZSET_TYPE:
			if _, err = tdb.ZRemRangeByScore(tikv_txn, key, utils.SCORE_MIN, utils.SCORE_MAX); err != nil {
				return
			}
		}
		//delete key
		if err = tikv_txn.Delete(key); err != nil {
			return
		}
		it.Next()
		loops--
		log.Debug(loops)
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
