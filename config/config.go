package config

import (
	"context"
	"github.com/naoina/toml"
	"os"
)

type Config struct {
	MySQLConfig struct {
		Database        string
		Uri             string
		Addr            string
		User            string
		Password        string
		DBName          string
		MaxIdleConns    int
		MaxOpenConns    int
		ConnMaxLifetime int
	}
	Elastic struct {
		Uri      string
		User     string
		Password string
	}

	Redis struct {
		Address  string
		Password string
		DB       int
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
