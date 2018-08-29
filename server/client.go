package server

import (
	"bufio"
	"context"
	"net"
	"strings"

	"github.com/chuangyou/qkv/qkverror"
	"github.com/chuangyou/qkv/tidis"
	"github.com/pingcap/tidb/kv"
	"github.com/siddontang/goredis"
	log "github.com/sirupsen/logrus"
)

type Command struct {
	cmd  string
	args [][]byte
}
type Client struct {
	cmd     string
	args    [][]byte
	cmds    []Command
	isAuth  bool
	auth    string
	conn    net.Conn
	br      *bufio.Reader
	bw      *bufio.Writer
	r       *goredis.RespReader
	w       *goredis.RespWriter
	tdb     *tidis.Tidis
	isTxn   bool
	txn     kv.Transaction
	respTxn []interface{}
}

//NewClient new a client for process redis protocol request
func NewClient(conn net.Conn, tdb *tidis.Tidis, auth string) *Client {
	client := new(Client)
	client.conn = conn
	client.auth = auth
	client.br = bufio.NewReaderSize(conn, 4096)
	client.bw = bufio.NewWriterSize(conn, 4096)
	client.r = goredis.NewRespReader(client.br)
	client.w = goredis.NewRespWriter(client.bw)
	client.tdb = tdb
	return client
}

//ProcessRequest parse protocol and process data
func (c *Client) ProcessRequest(req [][]byte) (err error) {
	var (
		command Command
	)
	log.Debugf("req:%v,%s", strings.ToUpper(string(req[0])), req[1:])
	if len(req) == 0 {
		c.cmd = ""
		c.args = nil
	} else {
		c.cmd = strings.ToUpper(string(req[0]))
		c.args = req[1:]
	}
	if c.cmd != "AUTH" {
		if !c.isAuth {
			c.FlushResp(qkverror.ErrorNoAuth)
			return nil
		}
	}
	log.Debugf("command: %s argc:%d", c.cmd, len(c.args))
	switch c.cmd {
	case "AUTH":
		if len(c.args) != 1 {
			c.FlushResp(qkverror.ErrorCommandParams)
		}
		if c.auth == "" {
			c.FlushResp(qkverror.ErrorServerNoAuthNeed)
		} else if string(c.args[0]) != c.auth {
			c.isAuth = false
			c.FlushResp(qkverror.ErrorAuthFailed)
		} else {
			c.isAuth = true
			c.w.FlushString("OK")
		}
		return nil
	case "MULTI":
		log.Debugf("client transaction")
		c.txn, err = c.tdb.NewTxn()
		if err != nil {
			c.resetTxn()
			c.w.FlushBulk(nil)
			return nil
		}
		c.isTxn = true
		c.cmds = []Command{}
		c.respTxn = []interface{}{}
		c.w.FlushString("OK")
		err = nil
		return
	case "EXEC":
		log.Debugf("command length : %d  txn:%v", len(c.cmds), c.isTxn)
		if len(c.cmds) == 0 || !c.isTxn {
			c.w.FlushBulk(nil)
			c.resetTxn()
			return nil
		}
		for _, cmd := range c.cmds {
			log.Debugf("execute command: %s", cmd.cmd)
			c.cmd = cmd.cmd
			c.args = cmd.args
			if err = c.execute(); err != nil {
				break
			}
		}
		if err != nil {
			c.txn.Rollback()
			c.w.FlushBulk(nil)
		} else {
			err = c.txn.Commit(context.Background())
			if err == nil {
				c.w.FlushArray(c.respTxn)
			} else {
				c.w.FlushBulk(nil)
			}
		}
		c.resetTxn()
		return nil
	case "DISCARD":
		// discard transactional commands
		if c.isTxn {
			err = c.txn.Rollback()
		}
		c.w.FlushString("OK")
		c.resetTxn()
		return err
	case "PING":
		if len(c.args) != 0 {
			c.FlushResp(qkverror.ErrorCommandParams)
		}
		c.w.FlushString("PONG")
		return nil
	}
	if c.isTxn {
		command = Command{cmd: c.cmd, args: c.args}
		c.cmds = append(c.cmds, command)
		log.Debugf("command:%s added to transaction queue, queue size:%d", c.cmd, len(c.cmds))
		c.w.FlushString("QUEUED")
	} else {
		c.execute()
	}
	return

}

func (c *Client) FlushResp(resp interface{}) error {
	err := c.Resp(resp)
	if err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *Client) Resp(resp interface{}) error {
	var (
		err error
	)
	if c.isTxn {
		c.respTxn = append(c.respTxn, resp)
	} else {
		switch v := resp.(type) {
		case []interface{}:
			err = c.w.WriteArray(v)
		case []byte:
			err = c.w.WriteBulk(v)
		case nil:
			err = c.w.WriteBulk(nil)
		case int64:
			err = c.w.WriteInteger(v)
		case string:
			err = c.w.WriteString(v)
		case error:
			err = c.w.WriteError(v)
		default:
			err = qkverror.ErrorUnknownType
		}
	}

	return err
}
func (c *Client) GetTxn() kv.Transaction {
	if c.isTxn {
		return c.txn
	} else {
		return nil
	}
}
func (c *Client) resetTxn() {
	c.isTxn = false
	c.cmds = []Command{}
	c.respTxn = []interface{}{}
}
func (c *Client) execute() error {
	var err error
	if len(c.cmd) == 0 {
		err = qkverror.ErrorCommand
	} else if f, ok := getCommandFunc(c.cmd); !ok {
		err = qkverror.ErrorCommand
	} else {
		err = f(c)
	}
	if err != nil && !c.isTxn {
		c.w.FlushError(err)
	}
	c.w.Flush()
	return err
}
