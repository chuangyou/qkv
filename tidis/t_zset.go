package tidis

import (
	"context"
	"strconv"

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
		zk             *ZSetPair
		ttl            uint64
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zsize          uint64
		zSetData       []byte
		zSetScore      []byte
		value          []byte
		encodeScore    []byte
		oldScore       int64
		oldScoreKey    []byte
		zSetValue      []byte
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
	//get zset meta
	zsize, ttl, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	for _, zk = range zks {
		//encode zset member
		zSetData = utils.EncodeZSetData(key, zk.Key)
		//encode zset member's score
		zSetScore = utils.EncodeZSetScore(key, zk.Key, zk.Score)
		//encode this score
		encodeScore, err = utils.Int64ToBytes(zk.Score)
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
		err = tikv_txn.Set(zSetData, encodeScore)
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
	// encode zset_type
	zSetValue = utils.EncodeData(utils.ZSET_TYPE, zSetValue)
	err = tikv_txn.Set(key, zSetValue)
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
		zsize uint64 = 0
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zsize, _, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	count = int64(zsize)
	return
}

//ZCount returns the number of elements in the sorted set at key with a score between min and max.
func (tidis *Tidis) ZCount(txn interface{}, key []byte, min, max int64) (count int64, err error) {
	var (
		zsize     uint64 = 0
		startKey  []byte
		endKey    []byte
		tempCount uint64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zsize, _, _, err = tidis.getZSetMeta(txn, key)
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
		newScore       int64
		oldScore       int64
		zSetValue      []byte
		oldScoreRaw    []byte
		newScoreRaw    []byte
		zSetMetaKey    []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	zsize, ttl, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	//member data
	zSetMetaKey = utils.EncodeZSetData(key, member)
	//old score bytes
	oldScoreRaw, err = tidis.db.Get(txn, zSetMetaKey)
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
		//set zset meta data
		newScoreRaw, _ = utils.Int64ToBytes(newScore)
		err = tikv_txn.Set(zSetMetaKey, newScoreRaw)
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
		//encode zset type
		zSetValue = utils.EncodeData(utils.ZSET_TYPE, zSetValue)
		err = tikv_txn.Set(key, zSetValue)
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
		err = tikv_txn.Set(zSetMetaKey, newScoreRaw)
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
		zsize     uint64
		startKey  []byte
		withStart bool
		endKey    []byte
		withEnd   bool
		tempCount uint64
	)
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if !utils.ChkPrefix(start) || !utils.ChkPrefix(stop) {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zsize, _, _, err = tidis.getZSetMeta(txn, key)
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
	count = int64(tempCount)
	return
}

//ZRange returns the specified range of elements in the sorted set stored at key.
func (tidis *Tidis) ZRange(txn interface{}, key []byte, start, stop int64, withscores bool, reverse bool) (resp []interface{}, err error) {
	var (
		startKey []byte
		endKey   []byte
		offset   int64
		count    int64
		members  [][]byte
		respLen  int
		score    int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if start > stop && (stop > 0 || start < 0) {
		resp = utils.EmptyListInterfaces
		return
	}

	//start key
	startKey = utils.EncodeZSetScore(key, []byte{0}, utils.SCORE_MIN)
	//end key
	endKey = utils.EncodeZSetScore(key, []byte{0}, utils.SCORE_MAX)
	//offset count
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

	return
}

//ZRangeByLex return a range of members in a sorted set, by lexicographical range
func (tidis *Tidis) ZRangeByLex(txn interface{}, key []byte, start, stop []byte, offset, count int, reverse bool) (resp []interface{}, err error) {
	var (
		startKey  []byte
		endKey    []byte
		withStart bool = true
		withEnd   bool = true
		zsize     uint64
		members   [][]byte
	)
	if len(key) == 0 || len(start) == 0 || len(stop) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zsize, _, _, err = tidis.getZSetMeta(txn, key)
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
		count = int(zsize)
	}
	if offset > int(zsize)-1 {
		resp = utils.EmptyListInterfaces
		return
	}
	if reverse {
		offset = int(zsize) - offset - count
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
	return
}

//ZRangeByScore returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
func (tidis *Tidis) ZRangeByScore(txn interface{}, key []byte, min, max int64, withscores bool, offset, count int, reverse bool) (resp []interface{}, err error) {
	var (
		zsize    uint64
		startKey []byte
		endKey   []byte
		members  [][]byte
		end      int
		respLen  int
		score    int64
	)
	if len(key) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	if (!reverse && min > max) || (reverse && min < max) {
		resp = utils.EmptyListInterfaces
		return
	}
	zsize, _, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	if zsize == 0 {
		resp = utils.EmptyListInterfaces
		return
	}
	if reverse {
		startKey = utils.EncodeZSetScore(key, []byte{0}, max)
		endKey = utils.EncodeZSetScore(key, []byte{0}, min+1)
	} else {
		startKey = utils.EncodeZSetScore(key, []byte{0}, min)
		endKey = utils.EncodeZSetScore(key, []byte{0}, max+1)
	}
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, endKey, true, 0, zsize, false)
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
	return
}

//ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
func (tidis *Tidis) ZRem(txn interface{}, key []byte, members ...[]byte) (deleted int64, err error) {
	var (
		tikv_txn       kv.Transaction
		ok             bool
		notTransaction bool
		zsize          uint64
		ttl            uint64
		score          int64
		zSetValue      []byte
		member         []byte
		zSetMetaKey    []byte
		zSetScoreKey   []byte
		scoreBytes     []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	zsize, ttl, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	for _, member = range members {
		zSetMetaKey = utils.EncodeZSetData(key, member)
		scoreBytes, err = tidis.db.Get(txn, zSetMetaKey)
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
		//delete meta data
		err = tikv_txn.Delete(zSetMetaKey)
		if err != nil {
			return
		}
		//delete score
		err = tikv_txn.Delete(zSetScoreKey)
		if err != nil {
			return
		}
	}
	if zsize < uint64(deleted) {
		err = qkverror.ErrorInvalidMeta
		return
	}
	//update meta
	zsize = zsize - uint64(deleted)
	zSetValue = tidis.createZSetMeta(zsize, ttl, utils.FLAG_NORMAL)
	//encode zset type
	zSetValue = utils.EncodeData(utils.ZSET_TYPE, zSetValue)
	err = tikv_txn.Set(key, zSetValue)
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
		zsize          uint64
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
		zSetValue      []byte
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
	} else {
		tikv_txn, ok = txn.(kv.Transaction)
		if !ok {
			err = qkverror.ErrorServerInternal
			return
		}
	}
	zsize, ttl, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	if zsize == 0 {
		return
	}
	startKey, withStart = tidis.zlexParse(key, start)
	endKey, withEnd = tidis.zlexParse(key, stop)
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, withStart, endKey, withEnd, 0, zsize, false)
	if err != nil {
		return
	}
	deleted = int64(len(members))
	if zsize < uint64(deleted) {
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
	zsize = zsize - uint64(deleted)
	if zsize == 0 {
		//delete key
		err = tikv_txn.Delete(key)
		if err != nil {
			return
		}
	} else {
		//update meta
		zSetValue = tidis.createZSetMeta(zsize, ttl, utils.FLAG_NORMAL)
		//encode zset type
		zSetValue = utils.EncodeData(utils.ZSET_TYPE, zSetValue)
		err = tikv_txn.Set(key, zSetValue)
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
		zsize          uint64
		ttl            uint64
		startKey       []byte
		endKey         []byte
		members        [][]byte
		member         []byte
		dMember        []byte
		zSetMetaKey    []byte
		zSetValue      []byte
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
	zsize, ttl, _, err = tidis.getZSetMeta(txn, key)
	if err != nil {
		return
	}
	if zsize == 0 {
		return
	}
	startKey = utils.EncodeZSetScore(key, []byte{0}, min)
	endKey = utils.EncodeZSetScore(key, []byte{0}, max+1)
	members, _, err = tidis.db.GetRangeKeys(txn, startKey, true, endKey, true, 0, zsize, false)
	if err != nil {
		return
	}
	for _, member = range members {
		_, dMember, _, err = utils.DecodeZSetScore(member)
		if err != nil {
			return
		}
		zSetMetaKey = utils.EncodeZSetData(key, dMember)
		err = tikv_txn.Delete(member)
		if err != nil {
			return
		}
		err = tikv_txn.Delete(zSetMetaKey)
		if err != nil {
			return
		}
	}
	deleted = int64(len(members))
	if zsize < uint64(deleted) {
		err = qkverror.ErrorInvalidMeta
		return
	}
	zsize = zsize - uint64(deleted)
	if zsize != 0 {
		//update meta
		zSetValue = tidis.createZSetMeta(zsize, ttl, utils.FLAG_NORMAL)
		zSetValue = utils.EncodeData(utils.ZSET_TYPE, zSetValue)
		err = tikv_txn.Set(key, zSetValue)
		if err != nil {
			return
		}
	} else {
		// delete meta
		err = tikv_txn.Delete(key)
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
		zSetMetaKey []byte
		scoreBytes  []byte
	)
	if len(key) == 0 || len(member) == 0 {
		err = qkverror.ErrorKeyEmpty
		return
	}
	zSetMetaKey = utils.EncodeZSetData(key, member)
	scoreBytes, err = tidis.db.Get(txn, zSetMetaKey)
	score, err = utils.BytesToInt64(scoreBytes)
	return
}

//zRangeParse zrange key offset count
func (tidis *Tidis) zRangeParse(txn interface{}, key []byte, start, stop int64, reverse bool) (offset int64, count int64, err error) {
	var (
		zsize uint64
		index int64
	)
	//get zset meta
	zsize, _, _, err = tidis.getZSetMeta(txn, key)
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
func (tidis *Tidis) createZSetMeta(size, ttl uint64, flag byte) []byte {
	return tidis.createHashMeta(size, ttl, flag)
}

func (tidis *Tidis) getZSetMeta(txn interface{}, key []byte) (ssize uint64, ttl uint64, flag byte, err error) {
	var (
		dataType byte
	)
	dataType, ssize, ttl, flag, err = tidis.getHashMeta(txn, key)
	if ssize > 0 {
		if dataType != utils.ZSET_TYPE {
			err = qkverror.ErrorWrongType
			return
		}
	}
	return
}
