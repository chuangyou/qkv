package config

import (
	"github.com/BurntSushi/toml"
)

type QKVConfig struct {
	Address              string `toml:"address"`
	MaxConnection        int    `toml:"max_connection"`
	Auth                 string `toml:"auth"`
	LogFile              string `toml:"logfile"`
	LogLevel             string `toml:"loglevel"`
	Maxproc              int    `toml:"maxproc"`
	StypeCheckerLoop     int    `toml:"stype_checker_loop"`
	StypeCheckerInterval int    `toml:"stype_checker_interval"`
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
