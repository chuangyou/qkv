package server

import (
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
)

func init() {
	commandRegister("LINDEX", lIndexCommand)
	commandRegister("LLEN", lLenCommand)
	commandRegister("LPOP", lPopCommand)
	commandRegister("LPUSH", lPushCommand)
	commandRegister("LRANGE", lRangeComamnd)
	commandRegister("LSET", lSetComamnd)
	commandRegister("LTRIM", lTrimCommand)
	commandRegister("RPOP", rPopCommand)
	commandRegister("RPUSH", rPushCommand)
}
func lIndexCommand(c *Client) (err error) {
	var (
		value []byte
		index int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		index, err = utils.StrBytesToInt64(c.args[1])
		if err != nil {
			return
		}
		value, err = c.tdb.LIndex(c.GetTxn(), c.args[0], index)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func lLenCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.LLen(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func lPopCommand(c *Client) (err error) {
	var (
		value []byte
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.LPop(c.GetTxn(), c.args[0], utils.LHeadDirection)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func lPushCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.LPush(c.GetTxn(), c.args[0], utils.LHeadDirection, c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func lRangeComamnd(c *Client) (err error) {
	var (
		start, end int64
		value      []interface{}
	)
	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	}
	start, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	end, err = utils.StrBytesToInt64(c.args[2])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	value, err = c.tdb.LRange(c.GetTxn(), c.args[0], start, end)
	if err != nil {
		return
	}
	return c.Resp(value)
}
func lSetComamnd(c *Client) (err error) {
	var (
		index int64
	)
	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	}
	index, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		return
	}
	err = c.tdb.LSet(c.GetTxn(), c.args[0], index, c.args[2])
	if err != nil {
		return
	}
	return c.Resp("OK")
}
func lTrimCommand(c *Client) (err error) {
	var (
		start, end int64
	)
	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	}
	start, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		return
	}
	end, err = utils.StrBytesToInt64(c.args[2])
	if err != nil {
		return
	}
	err = c.tdb.LTrim(c.GetTxn(), c.args[0], start, end)
	if err != nil {
		return
	}
	return c.Resp("OK")
}
func rPopCommand(c *Client) (err error) {
	var (
		value []byte
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.LPop(c.GetTxn(), c.args[0], utils.LTailDirection)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func rPushCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.LPush(c.GetTxn(), c.args[0], utils.LTailDirection, c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
