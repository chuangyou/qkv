package server

import (
	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/utils"
)

func init() {
	//redis SADD
	commandRegister("SADD", saddCommand)
	//redis SCARD
	commandRegister("SCARD", scardCommand)
	//redis SDIFF
	commandRegister("SDIFF", sdiffCommand)
	//redis SDIFFSTORE
	commandRegister("SDIFFSTORE", sdiffStoreCommand)
	//redis SINTER
	commandRegister("SINTER", sinterCommand)
	//redis SINTERSTORE
	commandRegister("SINTERSTORE", sinterStoreCommand)
	//redis SISMEMBER
	commandRegister("SISMEMBER", sismerCommand)
	//redis SMEMBERS
	commandRegister("SMEMBERS", smembersCommand)
	//TODO redis SRANDMEMBER
	//redis SREM
	commandRegister("SREM", sremCommand)
	//redis SUNION
	commandRegister("SUNION", sunionCommand)
	//extension SDEL,for del the set type key
	commandRegister("SDEL", sdelCommand)
	//extension SEXPIRE,for expire set type
	commandRegister("SEXPIRE", sexpireCommand)
	//extension SPEXPIRE,for pexpire set type
	commandRegister("SPEXPIRE", spexpireCommand)
	//extension SEXPIREAT,for expireat set type
	commandRegister("SEXPIREAT", sexpireatCommand)
	//extension SPEXPIREAT,for pexpireat set type
	commandRegister("SPEXPIREAT", spexpireatCommand)
	//extension STTL,for ttl set type
	commandRegister("STTL", sttlCommand)
	//extension SPTTL,for ttl set type
	commandRegister("SPTTL", spttlCommand)

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
func sdelCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) < 1 {
		err = qkverror.ErrorCommandParams
	} else {
		ret, err = c.tdb.SDel(c.GetTxn(), c.args...)
		if err != nil {
			return
		}
	}
	return c.Resp(ret)
}
func sexpireCommand(c *Client) (err error) {
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
	ret, err = c.tdb.SExpire(c.GetTxn(), c.args[0], seconds)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func spexpireCommand(c *Client) (err error) {
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
	ret, err = c.tdb.SPExpire(c.GetTxn(), c.args[0], ms)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func sexpireatCommand(c *Client) (err error) {
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
	ret, err = c.tdb.SExpireAt(c.GetTxn(), c.args[0], timestamp)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}
func spexpireatCommand(c *Client) (err error) {
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
	ret, err = c.tdb.SPExpireAt(c.GetTxn(), c.args[0], ms)
	if err != nil {
		return
	}
	return c.Resp(int64(ret))
}

func sttlCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.STTL(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(ret)
}
func spttlCommand(c *Client) (err error) {
	var (
		ret int64
	)
	if len(c.args) != 1 {
		err = qkverror.ErrorCommandParams
		return
	}
	ret, err = c.tdb.SPTTL(c.GetTxn(), c.args[0])
	if err != nil {
		return
	}
	return c.Resp(ret)
}
