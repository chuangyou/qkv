package server

import (
	"strings"

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
