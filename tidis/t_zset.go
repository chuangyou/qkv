package tidis

import (
	"context"
	"strconv"
	"time"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
	"github.com/pingcap/tidb/kv"
)

type ZSetPair struct {
	Score int64
	Key   []byte
}

//ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
func (tidis *Tidis) ZAdd(txn interface{}, key []byte, zks ...*ZSetPair) (added int64, err error) {
	var (
		zSetKey        []byte
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zk             *ZSetPair
		zSetData       []byte
		zSetScore      []byte
		score          []byte
		value          []byte
		zsize          uint64
		oldScore       int64
		oldScoreKey    []byte
		zSetValue      []byte
		ttl            uint64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	//encode zset key
	zSetKey = utils.EncodeZSetKey(key)
	//get zset meta
	zsize, ttl, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	for _, zk = range zks {
		//encode zset member
		zSetData = utils.EncodeZSetData(key, zk.Key)
		//encode zset member's score
		zSetScore = utils.EncodeZSetScore(key, zk.Key, zk.Score)
		//encode this score
		score, err = utils.Int64ToBytes(zk.Score)
		if err != nil {
			return
		}
		//get old score
		value, err = tidis.db.Get(txn, zSetData)
		if err != nil {
			return
		}
		if value == nil {
			//key member not exists
			zsize++
			added++
		} else {
			//delete old score key
			oldScore, err = utils.BytesToInt64(value)
			if err != nil {
				return
			}
			oldScoreKey = utils.EncodeZSetScore(key, zk.Key, oldScore)
			err = tikv_txn.Delete(oldScoreKey)
			if err != nil {
				return
			}
		}
		//set zset member
		err = tikv_txn.Set(zSetData, score)
		if err != nil {
			return
		}
		//set zset member's score
		err = tikv_txn.Set(zSetScore, []byte{0})
		if err != nil {
			return
		}
	}
	// update zset
	zSetValue = tidis.createZSetMeta(zsize, ttl, utils.FLAG_NORMAL)
	err = tikv_txn.Set(zSetKey, zSetValue)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (tidis *Tidis) ZCard(txn interface{}, key []byte) (count int64, err error) {
	var (
		zsize   uint64 = 0
		zSetKey []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zSetKey = utils.EncodeZSetKey(key)
	zsize, _, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	count = int64(zsize)
	return
}

//ZCount returns the number of elements in the sorted set at key with a score between min and max.
func (tidis *Tidis) ZCount(txn interface{}, key []byte, min, max int64) (count int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zsize          uint64 = 0
		zSetKey        []byte
		startKey       []byte
		endKey         []byte
		tempCount      uint64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	zSetKey = utils.EncodeZSetKey(key)
	zsize, _, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	if zsize == 0 {
		return
	}
	startKey = utils.EncodeZSetScore(key, []byte{0}, min)
	endKey = utils.EncodeZSetScore(key, []byte{0}, max+1)
	_, tempCount, err = tidis.db.GetRangeKeys(txn, startKey, true, endKey, true, 0, zsize, true)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	count = int64(tempCount)
	return
}

//ZIncrby increments the score of member in the sorted set stored at key by increment.
func (tidis *Tidis) ZIncrby(txn interface{}, key []byte, step int64, member []byte) (resp int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zsize          uint64 = 0
		ttl            uint64 = 0
		zSetKey        []byte
		zSetDataKey    []byte
		zSetValue      []byte
		oldScore       int64
		oldScoreRaw    []byte
		newScore       int64
		newScoreRaw    []byte
		zScoreKey      []byte
	)
	if len(key) == 0 || len(member) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}

	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	//meta data
	zSetKey = utils.EncodeZSetKey(key)
	zsize, ttl, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	//member data
	zSetDataKey = utils.EncodeZSetData(key, member)
	//old score bytes
	oldScoreRaw, err = tidis.db.Get(txn, zSetDataKey)
	if err != nil {
		return
	}
	if oldScoreRaw == nil {
		//zero old score
		zsize++
		//new score
		newScore = step
		//score key
		zScoreKey = utils.EncodeZSetScore(key, member, newScore)
		//set zset data
		newScoreRaw, _ = utils.Int64ToBytes(newScore)
		err = tikv_txn.Set(zSetDataKey, newScoreRaw)
		if err != nil {
			return
		}
		//set zset score key
		err = tikv_txn.Set(zScoreKey, []byte{0})
		if err != nil {
			return
		}
		//set zset meta data
		zSetValue = tidis.createSetMeta(zsize, ttl, utils.FLAG_NORMAL)
		err = tikv_txn.Set(zSetKey, zSetValue)
		if err != nil {
			return
		}
	} else {
		//get oldscore
		oldScore, _ = utils.BytesToInt64(oldScoreRaw)
		//new score
		newScore = oldScore + step
		//set zset data
		newScoreRaw, _ = utils.Int64ToBytes(newScore)
		err = tikv_txn.Set(zSetDataKey, newScoreRaw)
		if err != nil {
			return
		}
		// delete old score key
		zScoreKey = utils.EncodeZSetScore(key, member, oldScore)
		err = tikv_txn.Delete(zScoreKey)
		if err != nil {
			return
		}
		//set zset score key
		zScoreKey = utils.EncodeZSetScore(key, member, newScore)
		err = tikv_txn.Set(zScoreKey, []byte{0})
		if err != nil {
			return
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	resp = newScore
	return
}

//ZLexcount count the number of members in a sorted set between a given lexicographical range
func (tidis *Tidis) ZLexcount(txn interface{}, key, start, stop []byte) (count int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zSetKey        []byte
		zsize          uint64
		startKey       []byte
		withStart      bool
		endKey         []byte
		withEnd        bool
		tempCount      uint64
	)
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if !utils.ChkPrefix(start) || !utils.ChkPrefix(stop) {
		err = qkverror.ErrorKeyEmpty
		return
	}

	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	zSetKey = utils.EncodeZSetKey(key)
	zsize, _, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	if zsize == 0 {
		return
	}
	startKey, withStart = tidis.zlexParse(key, start)
	endKey, withEnd = tidis.zlexParse(key, stop)
	_, tempCount, err = tidis.db.GetRangeKeys(txn, startKey, withStart, endKey, withEnd, 0, zsize, true)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	count = int64(tempCount)
	return
}

//ZRange returns the specified range of elements in the sorted set stored at key.
func (tidis *Tidis) ZRange(txn interface{}, key []byte, start, stop int64, withscores bool, reverse bool) (resp []interface{}, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		startKey       []byte
		endKey         []byte
		offset         int64
		count          int64
		members        [][]byte
		respLen        int
		score          int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if start > stop && (stop > 0 || start < 0) {
		resp = utils.EmptyListInterfaces
		return
	}

	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}

	startKey = utils.EncodeZSetScore(key, []byte{0}, utils.SCORE_MIN)
	endKey = utils.EncodeZSetScore(key, []byte{0}, utils.SCORE_MAX)
	offset, count, err = tidis.zRangeParse(txn, key, start, stop, reverse)
	if err != nil {
		return
	}
	if offset == 0 && count == 0 {
		resp = utils.EmptyListInterfaces
		return
	}
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, endKey, true, uint64(offset), uint64(count), false)

	if err != nil {
		return
	}
	respLen = len(members)
	if withscores {
		respLen = respLen * 2
	}
	resp = make([]interface{}, respLen)
	if withscores {
		if !reverse {
			for i, idx := 0, 0; i < respLen; i, idx = i+2, idx+1 {
				_, resp[i], score, _ = utils.DecodeZSetScore(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(score, 10))
			}
		} else {
			for i, idx := respLen-2, 0; i >= 0; i, idx = i-2, idx+1 {
				_, resp[i], score, _ = utils.DecodeZSetScore(members[idx])

				resp[i+1] = []byte(strconv.FormatInt(score, 10))
			}
		}
	} else {
		if !reverse {
			for i, member := range members {
				_, resp[i], _, err = utils.DecodeZSetScore(member)
			}
		} else {
			for i, idx := len(members)-1, 0; i >= 0; i, idx = i-1, idx+1 {
				_, resp[idx], _, _ = utils.DecodeZSetScore(members[i])
			}
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZRangeByLex return a range of members in a sorted set, by lexicographical range
func (tidis *Tidis) ZRangeByLex(txn interface{}, key []byte, start, stop []byte, offset, count int, reverse bool) (resp []interface{}, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		startKey       []byte
		endKey         []byte
		withStart      bool = true
		withEnd        bool = true
		zSetKey        []byte
		zSize          uint64
		members        [][]byte
	)
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	zSetKey = utils.EncodeZSetKey(key)
	zSize, _, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	startKey, withStart = tidis.zlexParse(key, start)
	endKey, withEnd = tidis.zlexParse(key, start)
	switch stop[0] {
	case '-':
		endKey = utils.EncodeZSetData(key, []byte{0})
	case '+':
		endKey = utils.EncodeZSetDataEnd(key)
	case '(':
		endKey = utils.EncodeZSetData(key, stop[1:])
		withEnd = false
	case '[':
		endKey = utils.EncodeZSetData(key, stop[1:])
		withEnd = true
	}
	if count < 0 {
		count = int(zSize)
	}
	if offset > int(zSize)-1 {
		resp = utils.EmptyListInterfaces
		return
	}
	if reverse {
		offset = int(zSize) - offset - count
		if offset < 0 {
			count = count + offset
			offset = 0
		}
	}
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, withStart, endKey, withEnd, uint64(offset), uint64(count), false)
	resp = make([]interface{}, len(members))
	if reverse {
		for i, idx := 0, len(members)-1; idx >= 0; i, idx = i+1, idx-1 {
			_, resp[i], _ = utils.DecodeZSetData(members[idx])
		}
	} else {
		for i, member := range members {
			_, resp[i], _ = utils.DecodeZSetData(member)
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZRangeByScore returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
func (tidis *Tidis) ZRangeByScore(txn interface{}, key []byte, min, max int64, withscores bool, offset, count int, reverse bool) (resp []interface{}, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zSize          uint64
		members        [][]byte
		zSetKey        []byte
		startKey       []byte
		endKey         []byte
		end            int
		respLen        int
		score          int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if (!reverse && min > max) || (reverse && min < max) {
		resp = utils.EmptyListInterfaces
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	zSetKey = utils.EncodeZSetKey(key)
	if reverse {
		startKey = utils.EncodeZSetScore(key, []byte{0}, max)
		endKey = utils.EncodeZSetScore(key, []byte{0}, min+1)
	} else {
		startKey = utils.EncodeZSetScore(key, []byte{0}, min)
		endKey = utils.EncodeZSetScore(key, []byte{0}, max+1)
	}
	zSize, _, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	if zSize == 0 {
		resp = utils.EmptyListInterfaces
		return
	}
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, endKey, true, 0, zSize, false)
	if offset >= 0 {
		if offset < len(members) {
			if reverse {
				offset = len(members) - offset
				end = offset - count
				if end < 0 {
					end = 0
				}
				members = members[end:offset]
			} else {
				end = offset + count
				if end > len(members) {
					end = len(members)
				}
				members = members[offset:end]
			}
		} else {
			return
		}
	}
	respLen = len(members)
	if withscores {
		respLen = respLen * 2
	}
	resp = make([]interface{}, respLen)
	if withscores {
		if reverse {
			for i, idx := respLen-2, 0; i >= 0; i, idx = i-2, idx+1 {
				_, resp[i], score, _ = utils.DecodeZSetScore(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(score, 10))
			}
		} else {
			for i, idx := 0, 0; i < respLen; i, idx = i+2, idx+1 {
				_, resp[i], score, _ = utils.DecodeZSetScore(members[idx])
				resp[i+1] = []byte(strconv.FormatInt(score, 10))
			}
		}
	} else {
		if reverse {
			for i, idx := len(members)-1, 0; i >= 0; i, idx = i-1, idx+1 {
				_, resp[idx], _, _ = utils.DecodeZSetScore(members[i])
			}
		} else {
			for i, m := range members {
				_, resp[i], _, _ = utils.DecodeZSetScore(m)
			}
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
func (tidis *Tidis) ZRem(txn interface{}, key []byte, members ...[]byte) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zSize          uint64
		zSetKey        []byte
		zSetValue      []byte
		member         []byte
		zSetDataKey    []byte
		scoreBytes     []byte
		score          int64
		zSetScoreKey   []byte
		ttl            uint64
	)
	if len(key) == 0 || len(members) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	zSetKey = utils.EncodeZSetKey(key)
	zSize, ttl, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	for _, member = range members {
		zSetDataKey = utils.EncodeZSetData(key, member)
		scoreBytes, err = tidis.db.Get(txn, zSetDataKey)
		if err != nil {
			return
		}
		if scoreBytes == nil {
			continue
		}
		deleted++
		score, err = utils.BytesToInt64(scoreBytes)
		if err != nil {
			return
		}
		zSetScoreKey = utils.EncodeZSetScore(key, member, score)
		err = tikv_txn.Delete(zSetDataKey)
		if err != nil {
			return
		}
		err = tikv_txn.Delete(zSetScoreKey)
		if err != nil {
			return
		}
	}
	if zSize < uint64(deleted) {
		err = qkverror.ErrorInvalidMeta
		return
	}
	zSize = zSize - uint64(deleted)
	zSetValue = tidis.createZSetMeta(zSize, ttl, utils.FLAG_NORMAL)
	err = tikv_txn.Set(zSetKey, zSetValue)
	if err != nil {
		return
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZRemRangeByLex remove all members in a sorted set between the given lexicographical range
func (tidis *Tidis) ZRemRangeByLex(txn interface{}, key, start, stop []byte) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zSetKey        []byte
		zSetValue      []byte
		zSize          uint64
		ttl            uint64
		startKey       []byte
		endKey         []byte
		withStart      bool = true
		withEnd        bool = true
		members        [][]byte
		member         []byte
		dMember        []byte
		scoreBytes     []byte
		score          int64
		zSetScoreKey   []byte
	)
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	zSetKey = utils.EncodeZSetKey(key)
	zSize, ttl, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	if zSize == 0 {
		return
	}
	startKey, withStart = tidis.zlexParse(key, start)
	endKey, withEnd = tidis.zlexParse(key, stop)
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, withStart, endKey, withEnd, 0, zSize, false)
	if err != nil {
		return
	}
	deleted = int64(len(members))
	if zSize < uint64(deleted) {
		err = qkverror.ErrorInvalidMeta
		return
	}
	for _, member = range members {
		_, dMember, err = utils.DecodeZSetData(member)
		if err != nil {
			return
		}
		scoreBytes, err = tidis.Get(txn, member)
		if err != nil {
			return
		}
		score, _ = utils.BytesToInt64(scoreBytes)
		zSetScoreKey = utils.EncodeZSetScore(key, dMember, score)
		err = tikv_txn.Delete(member)
		if err != nil {
			return
		}
		err = tikv_txn.Delete(zSetScoreKey)
		if err != nil {
			return
		}
	}
	zSize = zSize - uint64(deleted)
	if zSize == 0 {
		//delete meta
		err = tikv_txn.Delete(zSetKey)
		if err != nil {
			return
		}
	} else {
		//update meta
		zSetValue = tidis.createZSetMeta(zSize, ttl, utils.FLAG_NORMAL)
		err = tikv_txn.Set(zSetKey, zSetValue)
		if err != nil {
			return
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZRemRangeByScore removes all elements in the sorted set stored at key with a score between min and max (inclusive).
func (tidis *Tidis) ZRemRangeByScore(txn interface{}, key []byte, min, max int64) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zSetKey        []byte
		zSetValue      []byte
		zSize          uint64
		ttl            uint64
		startKey       []byte
		endKey         []byte
		members        [][]byte
		member         []byte
		dMember        []byte
		zSetData       []byte
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	zSetKey = utils.EncodeZSetKey(key)
	startKey = utils.EncodeZSetScore(key, []byte{0}, min)
	endKey = utils.EncodeZSetScore(key, []byte{0}, max+1)
	zSize, ttl, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	if zSize == 0 {
		return
	}
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, endKey, true, 0, zSize, false)
	if err != nil {
		return
	}
	for _, member = range members {
		_, dMember, _, err = utils.DecodeZSetScore(member)
		if err != nil {
			return
		}
		zSetData = utils.EncodeZSetData(key, dMember)
		err = tikv_txn.Delete(member)
		if err != nil {
			return
		}
		err = tikv_txn.Delete(zSetData)
		if err != nil {
			return
		}
	}
	deleted = int64(len(members))
	if zSize < uint64(deleted) {
		err = qkverror.ErrorInvalidMeta
		return
	}
	zSize = zSize - uint64(deleted)
	if zSize != 0 {
		//update meta
		zSetValue = tidis.createZSetMeta(zSize, ttl, utils.FLAG_NORMAL)
		err = tikv_txn.Set(zSetKey, zSetValue)
		if err != nil {
			return
		}
	} else {
		// delete meta
		err = tikv_txn.Delete(zSetKey)
		if err != nil {
			return
		}
	}
	if notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZScore Returns the score of member in the sorted set at key.
func (tidis *Tidis) ZScore(txn interface{}, key, member []byte) (score int64, err error) {
	var (
		zSetDataKey []byte
		scoreBytes  []byte
	)
	if len(key) == 0 || len(member) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zSetDataKey = utils.EncodeZSetData(key, member)
	scoreBytes, err = tidis.db.Get(txn, zSetDataKey)
	score, err = utils.BytesToInt64(scoreBytes)
	return
}

//ZExpire set a key's time to live in seconds
func (tidis *Tidis) ZExpire(txn interface{}, key []byte, seconds int64) (ret int, err error) {
	var (
		ts             int64
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	ts = seconds*1000 + (time.Now().UnixNano() / 1000 / 1000)
	ret, err = tidis.db.ZPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZPExipre this command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of seconds.
func (tidis *Tidis) ZPExpire(txn interface{}, key []byte, ms int64) (ret int, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		ts             int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	ts = ms + (time.Now().UnixNano() / 1000 / 1000)
	ret, err = tidis.db.ZPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZExpireAt set the expiration for a key as a UNIX timestamp
func (tidis *Tidis) ZExpireAt(txn interface{}, key []byte, ts int64) (ret int, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	ts = ts * 1000
	ret, err = tidis.db.ZPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZPExpireAt set the expiration for a key as a UNIX timestamp specified in milliseconds
func (tidis *Tidis) ZPExpireAt(txn interface{}, key []byte, ts int64) (ret int, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if txn == nil {
		//start transaction
		notTransaction = true
		txn, err = tidis.NewTxn()
		if err != nil {
			return
		}
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
		defer tikv_txn.Rollback()
	}
	ret, err = tidis.db.ZPExipre(txn, key, ts)
	if err == nil && notTransaction {
		err = tikv_txn.Commit(context.Background())
		if err != nil {
			return
		}
	}
	return
}

//ZTTL returns the remaining time to live of a key that has a timeout.
func (tidis *Tidis) ZTTL(txn interface{}, key []byte) (ttl int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	return tidis.db.ZTTL(txn, key)
}

//ZPTTL get the time to live for a key in milliseconds
func (tidis *Tidis) ZPTTL(txn interface{}, key []byte) (ttl int64, err error) {
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	return tidis.db.ZPTTL(txn, key)
}

func (tidis *Tidis) createZSetMeta(size, ttl uint64, flag byte) []byte {
	return tidis.createHashMeta(size, ttl, flag)
}

func (tidis *Tidis) getZSetMeta(txn interface{}, key []byte) (ssize uint64, ttl uint64, flag byte, err error) {
	return tidis.getHashMeta(txn, key)
}

//zRangeParse zrange key offset count
func (tidis *Tidis) zRangeParse(txn interface{}, key []byte, start, stop int64, reverse bool) (offset int64, count int64, err error) {
	var (
		zsize   uint64
		zSetKey []byte
		index   int64
	)
	zSetKey = utils.EncodeZSetKey(key)
	//get zset meta
	zsize, _, _, err = tidis.getZSetMeta(txn, zSetKey)
	if err != nil {
		return
	}
	if zsize == 0 {
		return
	}

	index = int64(zsize)
	if start < 0 {
		if start < -index {
			start = 0
		} else {
			start = start + index
		}
	} else {
		if start >= index {
			return
		}
	}
	if stop < 0 {
		if stop < -index {
			stop = 0
		} else {
			stop = stop + index
		}
	} else {
		if stop >= index {
			stop = index - 1
		}
	}
	if reverse {
		start = index - stop - 1
		stop = index - start
		offset = start
		count = stop - start
	} else {
		offset = start
		count = stop - start + 1
	}
	return

}

func (tidis *Tidis) zlexParse(key, lex []byte) (lexKey []byte, ok bool) {
	if len(lex) == 0 {
		return
	}
	switch lex[0] {
	case '-':
		lexKey = utils.EncodeZSetData(key, []byte{0})
	case '+':
		lexKey = utils.EncodeZSetDataEnd(key)
	case '(':
		lexKey = utils.EncodeZSetData(key, lex[1:])
		ok = false
	case '[':
		lexKey = utils.EncodeZSetData(key, lex[1:])
		ok = true
	default:
		return
	}

	return
}
