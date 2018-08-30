package main

import (
	"flag"
	"os"

	"github.com/chuangyou/qkv/config"
	"github.com/chuangyou/qkv/server"
	log "github.com/sirupsen/logrus"
)

var (
	ConfigFile = flag.String("c", "./config.toml", "config filename")
)

func main() {
	flag.Parse()
	//init config
	conf := config.InitConfig(*ConfigFile)
	//init log
	if conf.QKV.LogFile != "" {
		file, err := os.OpenFile(conf.QKV.LogFile, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		log.SetOutput(file)
	}
	if conf.QKV.LogLevel == "debug" {
		log.SetLevel(log.DebugLevel)
	}
	if conf.QKV.LogLevel == "info" {
		log.SetLevel(log.InfoLevel)
	}
	qkvServer, err := server.NewServer(conf)
	if err != nil {
		panic(err)
	}
	go qkvServer.Start()
	go qkvServer.TTLCheck()
	InitSignal()

}
