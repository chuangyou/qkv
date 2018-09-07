package server

import (
	"net"

	"io"

	"github.com/chuangyou/qkv/config"
	"github.com/chuangyou/qkv/tidis"

	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

var serverConnCount int32 = 0

type Server struct {
	conf     *config.Config
	listener *net.TCPListener
	tdb      *tidis.Tidis
	auth     string
}

func NewServer(conf *config.Config) (server *Server, err error) {
	var (
		addr *net.TCPAddr
	)
	server = new(Server)
	server.conf = conf
	server.tdb, err = tidis.NewTidis(conf)
	server.auth = conf.QKV.Auth
	if addr, err = net.ResolveTCPAddr("tcp4", conf.QKV.Address); err != nil {
		log.Error("net.ResolveTCPAddr(\"tcp4\", \"%s\") error(%v)", conf.QKV.Address, err)
		return
	}
	if server.listener, err = net.ListenTCP("tcp4", addr); err != nil {
		log.Error("net.ListenTCP(\"tcp4\", \"%s\") error(%v)", conf.QKV.Address, err)
		return
	}
	return
}
func (s *Server) Start() {
	for i := 0; i < s.conf.QKV.Maxproc; i++ {
		go s.acceptTCP()
	}
}
func (s *Server) TTLCheck() {
	go tidis.TTLCheckerRun(s.tdb, s.conf.QKV.TTLCheckerLoop, s.conf.QKV.TTLCheckerInterval)

}
func (s *Server) acceptTCP() {
	var (
		conn   *net.TCPConn
		err    error
		client *Client
	)
	for {
		if conn, err = s.listener.AcceptTCP(); err != nil {
			// if listener close then return
			log.Error("listener.Accept(\"%s\") error(%v)", s.listener.Addr().String(), err)
			return
		}

		atomic.AddInt32(&serverConnCount, 1)
		if s.conf.QKV.MaxConnection > 0 {
			//check server max connection
			if atomic.LoadInt32(&serverConnCount) > s.conf.QKV.MaxConnection {
				log.Errorf("max server connection,disconn client ip:%s", conn.LocalAddr().String())
				conn.Close()
				return
			}
		}
		client = NewClient(conn, s.tdb, s.conf.QKV.Auth)
		go s.serveTCP(client)

	}
}
func (s *Server) serveTCP(client *Client) {
	defer atomic.AddInt32(&serverConnCount, -1)
	for {
		req, err := client.r.ParseRequest()
		if err != nil && err != io.EOF {
			log.Error(err.Error())
			return
		} else if err != nil {
			return
		}
		err = client.ProcessRequest(req)
		if err != nil && err != io.EOF {
			log.Error(err.Error())
			return
		}
	}
}
