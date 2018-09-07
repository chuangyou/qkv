package server

import (
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
)

func init() {
	commandRegister("GET", getCommand)
	commandRegister("SET", setCommand)
	commandRegister("MGET", mgetCommand)
	commandRegister("MSET", msetCommand)
	commandRegister("DEL", delCommand)
	commandRegister("SETEX", setEXCommand)
	commandRegister("INCR", incrCommand)
	commandRegister("INCRBY", incrbyCommand)
	commandRegister("DECR", decrCommand)
	commandRegister("DECRBY", decrbyCommand)
	commandRegister("STRLEN", strlenCommand)
	commandRegister("TTL", ttlCommand)
	commandRegister("PTTL", pttlCommand)
	commandRegister("EXPIRE", expireCommand)
	commandRegister("PEXPIRE", pexpireCommand)
	commandRegister("EXPIREAT", expireatCommand)
	commandRegister("PEXPIREAT", pexpireatCommand)
}
func getCommand(c *Client) (err error) {
	var (
		value []byte
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		value, err = c.tdb.Get(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func setCommand(c *Client) (err error) {
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	} else {
		err = c.tdb.Set(c.GetTxn(), c.args[0], c.args[1])
		if err != nil {
			return
		}
	}
	return c.Resp("OK")
}
func mgetCommand(c *Client) (err error) {
	var (
		data []interface{}
	)
	if len(c.args) < 1 {
		err = qkverror.ErrorCommandParams
	} else {
		data, err = c.tdb.MGet(c.GetTxn(), c.args)
		if err != nil {
			return
		} else {
			err = c.Resp(data)
		}
	}
	return
}
func msetCommand(c *Client) (err error) {
	if len(c.args) < 2 && len(c.args)%2 != 0 {
		err = qkverror.ErrorCommandParams
		return
	}
	_, err = c.tdb.MSet(c.GetTxn(), c.args)
	if err != nil {
		return
	}
	return c.Resp("OK")
}
func delCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.Delete(c.GetTxn(), c.args)
	if err != nil {
		return err
	}

	return c.Resp(int64(ret))

}
func setEXCommand(c *Client) (err error) {
	var (
		seconds int64
	)

	if len(c.args) != 3 {
		err = qkverror.ErrorCommandParams
		return
	}
	seconds, err = utils.StrBytesToInt64(c.args[1])
	if err != nil {
		err = qkverror.ErrorCommandParams
		return
	}
	if seconds < 0 {
		err = qkverror.ErrorCommandParams
		return
	}
	err = c.tdb.SetEX(c.GetTxn(), c.args[0], seconds, c.args[2])
	if err != nil {
		return
	}
	return c.Resp("OK")
}
func incrCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.Incr(c.GetTxn(), c.args[0], 1)
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func incrbyCommand(c *Client) (err error) {
	var (
		ret  int64
		step int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	}
	step, err = utils.StrBytesToInt64(c.args[1])
	ret, err = c.tdb.Incr(c.GetTxn(), c.args[0], step)
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func decrCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.Decr(c.GetTxn(), c.args[0], 1)
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func decrbyCommand(c *Client) (err error) {
	var (
		ret  int64
		step int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
		return
	}
	step, err = utils.StrBytesToInt64(c.args[1])
	ret, err = c.tdb.Decr(c.GetTxn(), c.args[0], step)
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func strlenCommand(c *Client) (err error) {
	var (
		value []byte
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	value, err = c.tdb.Get(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(int64(len(value)))
}
func ttlCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.TTL(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func pttlCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.PTTL(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func expireCommand(c *Client) (err error) {
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
	ret, err = c.tdb.Expire(c.GetTxn(), c.args[0], seconds)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func pexpireCommand(c *Client) (err error) {
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
	ret, err = c.tdb.PExpire(c.GetTxn(), c.args[0], ms)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func expireatCommand(c *Client) (err error) {
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
	ret, err = c.tdb.ExpireAt(c.GetTxn(), c.args[0], timestamp)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func pexpireatCommand(c *Client) (err error) {
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
	ret, err = c.tdb.PExpireAt(c.GetTxn(), c.args[0], ms)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
