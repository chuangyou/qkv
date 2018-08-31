package server

import (
	"net"

	"io"

	"github.com/chuangyou/qkv/config"
	"github.com/chuangyou/qkv/tidis"

	log "github.com/sirupsen/logrus"
)

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
	go tidis.StringTTLCheckerRun(s.conf.QKV.StringCheckerLoop, s.conf.QKV.StringCheckerInterval, s.tdb)
	go tidis.SetTTLCheckerRun(s.conf.QKV.SetCheckerLoop, s.conf.QKV.SetCheckerInterval, s.tdb)
	go tidis.ZSetTTLCheckerRun(s.conf.QKV.ZSetCheckerLoop, s.conf.QKV.ZSetCheckerInterval, s.tdb)

}
func (s *Server) acceptTCP() {
	var (
		conn    *net.TCPConn
		err     error
		connNum int
	)
	for {
		if conn, err = s.listener.AcceptTCP(); err != nil {
			// if listener close then return
			log.Error("listener.Accept(\"%s\") error(%v)", s.listener.Addr().String(), err)
			return
		}
		client := NewClient(conn, s.tdb, s.conf.QKV.Auth)
		go s.serveTCP(client)
		if connNum++; connNum >= s.conf.QKV.MaxConnection {
			connNum = 0
		}
	}
}
func (s *Server) serveTCP(client *Client) {
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
