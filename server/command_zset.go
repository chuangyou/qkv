package server

import (
	"strings"

	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/tidis"
	"github.com/chuangyou/qkv/utils"
)

func init() {
	//redis ZADD
	commandRegister("ZADD", zaddCommand)
	//redis ZCARD
	commandRegister("ZCARD", zcardCommand)
	//redis ZCOUNT
	commandRegister("ZCOUNT", zcountCommand)
	//redis ZINCRBY
	commandRegister("ZINCRBY", zincrbyCommand)
	//redis ZLEXCOUNT
	commandRegister("ZLEXCOUNT", zlexcountCommand)
	//redis ZRANGE
	commandRegister("ZRANGE", zrangeCommand)
	//redis ZRANGEBYLEX
	commandRegister("ZRANGEBYLEX", zrangeByLexCommand)
	//redis ZRANGEBYSCORE
	commandRegister("ZRANGEBYSCORE", zrangeByScoreCommand)
	//redis ZREM
	commandRegister("ZREM", zremCommand)
	//redis ZREMRANGEBYLEX
	commandRegister("ZREMRANGEBYLEX", zRemRangeByLexCommand)
	//redis ZREMRANGEBYSCORE
	commandRegister("ZREMRANGEBYSCORE", zRemRangeByScoreCommand)
	//redis ZREVRANGE
	commandRegister("ZREVRANGE", zRevRangeCommand)
	//redis ZREVRANGEBYLEX
	commandRegister("ZREVRANGEBYLEX", zRevRangeByLexCommand)
	//redis ZREVRANGEBYSCORE
	commandRegister("ZREVRANGEBYSCORE", zRevRangeByScoreCommand)
	//redis ZSCORE
	commandRegister("ZSCORE", zScoreCommand)
	//extension ZDEL,for del the zset type key
	commandRegister("ZDEL", zdelCommand)

	//extension ZEXPIRE,for expire zset type
	commandRegister("ZEXPIRE", zexpireCommand)
	//extension ZPEXPIRE,for pexpire zset type
	commandRegister("ZPEXPIRE", zpexpireCommand)
	//extension ZEXPIREAT,for expireat zset type
	commandRegister("ZEXPIREAT", zexpireatCommand)
	//extension ZPEXPIREAT,for pexpireat zset type
	commandRegister("ZPEXPIREAT", zpexpireatCommand)
	//extension ZTTL,for ttl zset type
	commandRegister("ZTTL", zttlCommand)
	//extension ZPTTL,for ttl zset type
	commandRegister("ZPTTL", zpttlCommand)

}
func zaddCommand(c *Client) (err error) {
	var (
		ret   int64
		zks   = make([]*tidis.ZSetPair, 0)
		zk    *tidis.ZSetPair
		score int64
	)
	if len(c.args) < 3 && len(c.args)%2 == 0 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		for i := 1; i < len(c.args); i += 2 {
			score, err = utils.StrBytesToInt64(c.args[i])
			if err != nil {
				return
			}
			zk = new(tidis.ZSetPair)
			zk.Key = c.args[i+1]
			zk.Score = score
			zks = append(zks, zk)
		}
		ret, err = c.tdb.ZAdd(c.GetTxn(), c.args[0], zks...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func zcardCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.ZCard(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}

	return c.Resp(ret)
}
func zcountCommand(c *Client) (err error) {
	var (
		min, max    int64
		scoreString string
		ret         int64
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		// score pre-process
		scoreString = strings.ToUpper(string(c.args[1]))
		switch scoreString {
		case "-INF":
			min = utils.SCORE_MIN
		case "+INF":
			min = utils.SCORE_MAX
		default:
			min, err = utils.StrBytesToInt64(c.args[1])
			if err != nil {
				return
			}
		}
		scoreString = strings.ToUpper(string(c.args[2]))
		switch scoreString {
		case "-INF":
			max = utils.SCORE_MIN
		case "+INF":
			max = utils.SCORE_MAX
		default:
			max, err = utils.StrBytesToInt64(c.args[2])
			if err != nil {
				return
			}
		}
		ret, err = c.tdb.ZCount(c.GetTxn(), c.args[0], min, max)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)

}
func zincrbyCommand(c *Client) (err error) {
	var (
		step  int64
		value int64
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		step, err = utils.StrBytesToInt64(c.args[1])
		if err != nil {
			return
		}
		value, err = c.tdb.ZIncrby(c.GetTxn(), c.args[0], step, c.args[2])
	}
	return c.Resp(value)
}
func zlexcountCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.ZLexcount(c.GetTxn(), c.args[0], c.args[1], c.args[2])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func zrangeCommand(c *Client) (err error) {
	var (
		value      []interface{}
		withscores bool
		start      int64
		end        int64
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		if len(c.args) == 4 {
			if strings.ToUpper(string(c.args[3])) == "WITHSCORES" {
				withscores = true
			} else {
				err = qkverror.ErrorCommandParams
				return
			}
		}
		start, err = utils.StrBytesToInt64(c.args[1])
		if err != nil {
			return
		}
		end, err = utils.StrBytesToInt64(c.args[2])
		if err != nil {
			return
		}

		value, err = c.tdb.ZRange(c.GetTxn(), c.args[0], start, end, withscores, false)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func zrangeByLexCommand(c *Client) (err error) {
	var (
		value  []interface{}
		offset int64 = 0
		count  int64 = -1
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		if len(c.args) > 3 {
			if len(c.args) != 6 {
				err = qkverror.ErrorCommandParams
				return
			}
			if strings.ToUpper(string(c.args[3])) != "LIMIT" {
				err = qkverror.ErrorCommandParams
				return
			}
			offset, err = utils.StrBytesToInt64(c.args[4])
			if err != nil {
				return
			}
			count, err = utils.StrBytesToInt64(c.args[5])
			if err != nil {
				return
			}
			if offset < 0 || count < 0 {
				err = qkverror.ErrorCommandParams
				return
			}
		}
		value, err = c.tdb.ZRangeByLex(c.GetTxn(), c.args[0], c.args[1], c.args[2], int(offset), int(count), false)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func zrangeByScoreCommand(c *Client) (err error) {
	var (
		value       []interface{}
		start       int64
		end         int64
		withscores  bool  = false
		offset      int64 = -1
		count       int64 = -1
		str         string
		scoreString string
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		for i := 3; i < len(c.args); i++ {
			str = strings.ToUpper(string(c.args[i]))
			if str == "WITHSCORES" {
				withscores = true
			} else if str == "LIMIT" {
				if len(c.args) <= i+2 {
					err = qkverror.ErrorCommandParams
					return
				}
				offset, err = utils.StrBytesToInt64(c.args[i+1])
				if err != nil {
					return
				}
				count, err = utils.StrBytesToInt64(c.args[i+2])
				if err != nil {
					return
				}
				break
			}
		}
		scoreString = strings.ToUpper(string(c.args[1]))
		switch scoreString {
		case "-INF":
			start = utils.SCORE_MIN
		case "+INF":
			start = utils.SCORE_MAX
		default:
			start, err = utils.StrBytesToInt64(c.args[1])
			if err != nil {
				return
			}
		}

		scoreString = strings.ToUpper(string(c.args[2]))
		switch scoreString {
		case "-INF":
			end = utils.SCORE_MIN
		case "+INF":
			end = utils.SCORE_MAX
		default:
			end, err = utils.StrBytesToInt64(c.args[2])
			if err != nil {
				return
			}
		}
		value, err = c.tdb.ZRangeByScore(c.GetTxn(), c.args[0], start, end, withscores, int(offset), int(count), false)
		if err != nil {
			return
		}

	}
	return c.Resp(value)
}
func zremCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.ZRem(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func zRemRangeByLexCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.ZRemRangeByLex(c.GetTxn(), c.args[0], c.args[1], c.args[2])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func zRemRangeByScoreCommand(c *Client) (err error) {
	var (
		ret         int64
		start       int64
		end         int64
		scoreString string
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		scoreString = strings.ToUpper(string(c.args[1]))
		switch scoreString {
		case "-INF":
			start = utils.SCORE_MIN
		case "+INF":
			start = utils.SCORE_MAX
		default:
			start, err = utils.StrBytesToInt64(c.args[1])
			if err != nil {
				return
			}
		}
		scoreString = strings.ToUpper(string(c.args[2]))
		switch scoreString {
		case "-INF":
			end = utils.SCORE_MIN
		case "+INF":
			end = utils.SCORE_MAX
		default:
			end, err = utils.StrBytesToInt64(c.args[2])
			if err != nil {
				return
			}
		}
		ret, err = c.tdb.ZRemRangeByScore(c.GetTxn(), c.args[0], start, end)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func zRevRangeCommand(c *Client) (err error) {
	var (
		value      []interface{}
		withscores bool
		start      int64
		end        int64
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		if len(c.args) == 4 {
			if strings.ToUpper(string(c.args[3])) == "WITHSCORES" {
				withscores = true
			} else {
				err = qkverror.ErrorCommandParams
				return
			}
		}
		start, err = utils.StrBytesToInt64(c.args[1])
		if err != nil {
			return
		}
		end, err = utils.StrBytesToInt64(c.args[2])
		if err != nil {
			return
		}

		value, err = c.tdb.ZRange(c.GetTxn(), c.args[0], start, end, withscores, true)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func zRevRangeByLexCommand(c *Client) (err error) {
	var (
		value  []interface{}
		offset int64 = 0
		count  int64 = -1
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		if len(c.args) > 3 {
			if len(c.args) != 6 {
				err = qkverror.ErrorCommandParams
				return
			}
			if strings.ToUpper(string(c.args[3])) != "LIMIT" {
				err = qkverror.ErrorCommandParams
				return
			}
			offset, err = utils.StrBytesToInt64(c.args[4])
			if err != nil {
				return
			}
			count, err = utils.StrBytesToInt64(c.args[5])
			if err != nil {
				return
			}
			if offset < 0 || count < 0 {
				err = qkverror.ErrorCommandParams
				return
			}
		}
		value, err = c.tdb.ZRangeByLex(c.GetTxn(), c.args[0], c.args[1], c.args[2], int(offset), int(count), true)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func zRevRangeByScoreCommand(c *Client) (err error) {
	var (
		value       []interface{}
		start       int64
		end         int64
		withscores  bool  = false
		offset      int64 = -1
		count       int64 = -1
		str         string
		scoreString string
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		for i := 3; i < len(c.args); i++ {
			str = strings.ToUpper(string(c.args[i]))
			if str == "WITHSCORES" {
				withscores = true
			} else if str == "LIMIT" {
				if len(c.args) <= i+2 {
					err = qkverror.ErrorCommandParams
					return
				}
				offset, err = utils.StrBytesToInt64(c.args[i+1])
				if err != nil {
					return
				}
				count, err = utils.StrBytesToInt64(c.args[i+2])
				if err != nil {
					return
				}
				break
			}
		}
		scoreString = strings.ToUpper(string(c.args[1]))
		switch scoreString {
		case "-INF":
			start = utils.SCORE_MIN
		case "+INF":
			start = utils.SCORE_MAX
		default:
			start, err = utils.StrBytesToInt64(c.args[1])
			if err != nil {
				return
			}
		}

		scoreString = strings.ToUpper(string(c.args[2]))
		switch scoreString {
		case "-INF":
			end = utils.SCORE_MIN
		case "+INF":
			end = utils.SCORE_MAX
		default:
			end, err = utils.StrBytesToInt64(c.args[2])
			if err != nil {
				return
			}
		}
		value, err = c.tdb.ZRangeByScore(c.GetTxn(), c.args[0], start, end, withscores, int(offset), int(count), true)
		if err != nil {
			return
		}

	}
	return c.Resp(value)
}
func zScoreCommand(c *Client) (err error) {
	var (
		value int64
		data  []byte
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.ZScore(c.GetTxn(), c.args[0], c.args[1])
		if err != nil {
			return
		}
		data = strconv.AppendInt([]byte(nil), value, 10)
	}
	return c.Resp(data)
}
func zdelCommand(c *Client) (err error) {
	var (
		ret int64
	)
	log.Debug(len(c.args))
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.ZRemRangeByScore(c.GetTxn(), c.args[0], utils.SCORE_MIN, utils.SCORE_MAX)
		if err != nil {
			return
		}
	}
	if ret > 1 {
		ret = 1
	}

	return c.Resp(ret)
}
func zexpireCommand(c *Client) (err error) {
	var (
		ret     int
		seconds int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	}
	seconds, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.ZExpire(c.GetTxn(), c.args[0], seconds)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func zpexpireCommand(c *Client) (err error) {
	var (
		ret int
		ms  int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	}
	ms, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.ZPExpire(c.GetTxn(), c.args[0], ms)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func zexpireatCommand(c *Client) (err error) {
	var (
		ret       int
		timestamp int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	}
	timestamp, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.ZExpireAt(c.GetTxn(), c.args[0], timestamp)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func zpexpireatCommand(c *Client) (err error) {
	var (
		ret int
		ms  int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	}
	ms, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.ZPExpireAt(c.GetTxn(), c.args[0], ms)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}

func zttlCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.ZTTL(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func zpttlCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.ZPTTL(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(ret)
}
