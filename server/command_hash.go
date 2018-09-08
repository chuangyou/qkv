package server

import (
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
)

func init() {
	commandRegister("HDEL", hdelCommand)
	commandRegister("HEXISTS", hexistsCommand)
	commandRegister("HGET", hgetCommand)
	commandRegister("HGETALL", hgetallCommand)
	commandRegister("HINCRBY", hincrbyCommand)
	commandRegister("HKEYS", hkeysCommand)
	commandRegister("HLEN", hlenCommand)
	commandRegister("HMGET", hmgetCommand)
	commandRegister("HMSET", hmsetCommand)
	commandRegister("HSET", hsetCommand)
	commandRegister("HSETNX", hsetnxCommand)
	commandRegister("HSTRLEN", hstrlenCommand)
	commandRegister("HVALS", hvalsCommand)
}
func hdelCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.HDel(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func hexistsCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.HExists(c.GetTxn(), c.args[0], c.args[1])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func hgetCommand(c *Client) (err error) {
	var (
		value []byte
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.HGet(c.GetTxn(), c.args[0], c.args[1])
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func hgetallCommand(c *Client) (err error) {
	var (
		value []interface{}
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.HGetAll(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func hincrbyCommand(c *Client) (err error) {
	var (
		step  int64
		value int64
	)
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		step, err = utils.StrBytesToInt64(c.args[2])
		if err != nil {
			return
		}
		value, err = c.tdb.HIncrby(c.GetTxn(), c.args[0], c.args[1], step)
	}
	return c.Resp(value)
}
func hkeysCommand(c *Client) (err error) {
	var (
		value []interface{}
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.HKeys(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func hlenCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.HLen(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func hmgetCommand(c *Client) (err error) {
	var (
		value []interface{}
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.HMGet(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func hmsetCommand(c *Client) (err error) {
	if len(c.args) < 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		err = c.tdb.HMSet(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp("OK")
}
func hsetCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.HSet(c.GetTxn(), c.args[0], c.args[1], c.args[2])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func hsetnxCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.HSetNX(c.GetTxn(), c.args[0], c.args[1], c.args[2])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func hstrlenCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		ret, err = c.tdb.HStrlen(c.GetTxn(), c.args[0], c.args[1])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func hvalsCommand(c *Client) (err error) {
	var (
		value []interface{}
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.HVals(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
