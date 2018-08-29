package store

import (
	"github.com/chuangyou/qkv/config"
	"github.com/chuangyou/qkv/store/tikv"
)

func Open(conf *config.Config) (DB, error) {
	db, err := tikv.Open(conf)
	if err != nil {
		return nil, err
	}
	return db, nil
}
