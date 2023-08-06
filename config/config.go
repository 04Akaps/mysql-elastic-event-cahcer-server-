package config

import (
	"context"
	"github.com/naoina/toml"
	"os"
)

type Config struct {
	MySQLConfig struct {
		Database        string
		DBName          string
		URI             string
		MaxIdleConns    int
		MaxOpenConns    int
		ConnMaxLifetime int
	}
	Elastic struct {
		Uri      string
		User     string
		Password string
	}

	CancelContext     context.Context
	CancelContextFunc context.CancelFunc
}

func NewConfig(file string) *Config {
	c := new(Config)

	if f, err := os.Open(file); err != nil {
		panic(err)
	} else {
		if err = toml.NewDecoder(f).Decode(c); err != nil {
			panic(err)
		} else {

			ctx := context.Background()

			c.CancelContext, c.CancelContextFunc = context.WithCancel(ctx)
			return c
		}
	}
}
