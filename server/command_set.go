package server

import (
	"github.com/chuangyou/qkv/qkverror"
)

func init() {
	//redis sadd
	commandRegister("SADD", saddCommand)
	//redis scard
	commandRegister("SCARD", scardCommand)
	//redis SDIFF
	commandRegister("SDIFF", sdiffCommand)
	//redis SDIFFSTORE
	commandRegister("SDIFFSTORE", sdiffStoreCommand)
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
	return
}
