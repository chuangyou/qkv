package tidis

import (
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

//DeleteWithTxn removes the specified keys. A key is ignored if it does not exist.
func (tidis *Tidis) DeleteWithTxn(txn interface{}, keys [][]byte) (deleted int64, err error) {
	var (
		ok       bool
		tikv_txn kv.Transaction
		rawData  []byte
		dataType byte
	)
	if txn == nil {
		err = qkverror.ErrorServerInternal
		return
	}
	tikv_txn, ok = txn.(kv.Transaction)
	if !ok {
		err = qkverror.ErrorServerInternal
		return
	}
	for _, k := range keys {
		rawData, _ = tidis.db.Get(txn, k)
		if rawData != nil {
			dataType, _, err = utils.DecodeData(rawData)
			if err != nil {
				return
			}
			switch dataType {
			//delete set member
			case utils.SET_TYPE:
				_, err = tidis.ClearSetMembers(txn, k)
			//delete zset member
			case utils.ZSET_TYPE:
				_, err = tidis.ZRemRangeByScore(txn, k, utils.SCORE_MIN, utils.SCORE_MAX)
				//delete hash field
			case utils.HASH_TYPE:
				_, err = tidis.ClearHash(txn, k)
				//delete list member
			}
			if err != nil {
				return
			}
			deleted++
		}

		err = tikv_txn.Delete(k)
		if err != nil {
			deleted = 0
			return
		}
	}
	return
}
