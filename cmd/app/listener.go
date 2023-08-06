package app

import (
	"fmt"
	"github.com/inconshreveable/log15"
	"mysql-event-cacher/config"
	"mysql-event-cacher/repository"
	"mysql-event-cacher/repository/elasticSearch"
	"mysql-event-cacher/repository/mysql"
	"os"
)

type Listener struct {
	config  *config.Config
	logger  log15.Logger
	mysql   *mysql.MySql
	elastic *elasticSearch.Elastic
}

func NewListener(cfg *config.Config) {
	listener := &Listener{
		config: cfg,
		logger: log15.New("module", "app/listener"),
	}
	var err error

	if listener.mysql, err = mysql.NewMySql(cfg); err != nil {
		listener.logger.Crit("MySql Connection Failed", err)
		os.Exit(0)
	}

	if listener.elastic, err = elasticSearch.NewElastic(cfg); err != nil {
		listener.logger.Crit("ElasticSearch Connection Failed", err)
		os.Exit(0)
	}

	listener.logger.Info("Connection All Success! Let's Code")

	repository.NewEventCatch(cfg, listener.mysql, listener.elastic)

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
