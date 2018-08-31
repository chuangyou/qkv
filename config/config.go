package config

import (
	"github.com/BurntSushi/toml"
)

type QKVConfig struct {
	Address               string `toml:"address"`
	MaxConnection         int    `toml:"max_connection"`
	Auth                  string `toml:"auth"`
	LogFile               string `toml:"logfile"`
	LogLevel              string `toml:"loglevel"`
	Maxproc               int    `toml:"maxproc"`
	StringCheckerLoop     int    `toml:"string_checker_loop"`
	StringCheckerInterval int    `toml:"string_checker_interval"`
	SetCheckerLoop        int    `toml:"set_checker_loop"`
	SetCheckerInterval    int    `toml:"set_checker_interval"`
	ZSetCheckerLoop       int    `toml:"zset_checker_loop"`
	ZSetCheckerInterval   int    `toml:"zset_checker_interval"`
}
type TikvConfig struct {
	Pds string `toml:"pds"`
}
type Config struct {
	QKV  QKVConfig  `toml:"qkv"`
	Tikv TikvConfig `toml:"tikv"`
}

func InitConfig(configFile string) (conf *Config) {
	conf = new(Config)
	if _, err := toml.DecodeFile(configFile, conf); err != nil {
		panic(err)
	}
	return

}
