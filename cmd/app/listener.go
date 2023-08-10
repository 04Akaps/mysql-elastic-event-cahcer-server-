package app

import (
	"fmt"
	"github.com/inconshreveable/log15"
	"mysql-event-cacher/config"
	"mysql-event-cacher/repository"
	"mysql-event-cacher/repository/elasticSearch"
	"mysql-event-cacher/repository/mysql"
	"mysql-event-cacher/repository/redis"
	"os"
	"time"
)

type Listener struct {
	config  *config.Config
	logger  log15.Logger
	mysql   *mysql.MySql
	elastic *elasticSearch.Elastic
	redis   *redis.RedisClient
}

func NewListener(cfg *config.Config) {
	listener := &Listener{
		config: cfg,
		logger: log15.New("module", "app/listener"),
	}
	var err error

	if listener.mysql, err = mysql.NewMySql(cfg); err != nil {
		listener.logger.Crit("MySql Connection Failed", "crit", err)
		os.Exit(0)
	}

	if listener.elastic, err = elasticSearch.NewElastic(cfg); err != nil {
		listener.logger.Crit("ElasticSearch Connection Failed", "crit", err)
		os.Exit(0)
	}

	if listener.redis, err = redis.NewRedisClient(cfg, 5*time.Minute); err != nil {
		listener.logger.Crit("Redis Connection Failed", "crit", err)
		os.Exit(0)
	}

	listener.logger.Info("Connection All Success! Let's Code")

	if err = repository.NewEventCatch(cfg, listener.mysql, listener.elastic, listener.redis); err != nil {
		listener.logger.Crit("Redis NewEventCatch Failed", "crit", err)
		os.Exit(0)
	}

	listener.waitUntilBug()
}

func (l *Listener) waitUntilBug() {
	fmt.Println("Event Listner Server Started")
	for {
		select {
		case <-l.config.CancelContext.Done():
			return
		}
	}
}
