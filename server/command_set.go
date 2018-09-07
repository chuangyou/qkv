package server

import (
	"github.com/chuangyou/qkv/qkverror"
)

func init() {
	commandRegister("SADD", saddCommand)
	commandRegister("SCARD", scardCommand)
	commandRegister("SDIFF", sdiffCommand)
	commandRegister("SDIFFSTORE", sdiffStoreCommand)
	commandRegister("SINTER", sinterCommand)
	commandRegister("SINTERSTORE", sinterStoreCommand)
	commandRegister("SISMEMBER", sismerCommand)
	commandRegister("SMEMBERS", smembersCommand)
	commandRegister("SREM", sremCommand)
	commandRegister("SUNION", sunionCommand)
}
func saddCommand(c *Client) (err error) {
	var (
		ret int
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SAdd(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(int64(ret))
}
func scardCommand(c *Client) (err error) {
	var (
		value int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
	} else {
		value, err = c.tdb.SCard(c.GetTxn(), c.args[0])
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
func sdiffCommand(c *Client) (err error) {
	var (
		ret []interface{}
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SDiff(c.GetTxn(), c.args...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func sdiffStoreCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SDiffStore(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func sinterCommand(c *Client) (err error) {
	var (
		ret []interface{}
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SInter(c.GetTxn(), c.args...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func sinterStoreCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SInterStore(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func sismerCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.Sismember(c.GetTxn(), c.args[0], c.args[1])
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func smembersCommand(c *Client) (err error) {
	var (
		value []interface{}
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
	} else {
		value, err = c.tdb.SMembers(c.GetTxn(), c.args[0])
		if err != nil {
			return err
		}
	}
	return c.Resp(value)
}
func sremCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SRem(c.GetTxn(), c.args[0], c.args[1:]...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func sunionCommand(c *Client) (err error) {
	var (
		value []interface{}
	)
	if len(c.args) < 2 {
		err = qkverror.ErrorCommandParams
	} else {
		value, err = c.tdb.SUnion(c.GetTxn(), c.args...)
		if err != nil {
			return
		}
	}
	return c.Resp(value)
}
