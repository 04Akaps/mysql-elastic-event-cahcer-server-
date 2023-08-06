package app

import (
	"fmt"
	"mysql-event-cacher/config"
	"mysql-event-cacher/repository/elasticSearch"
	"mysql-event-cacher/repository/mysql"
)

type Listener struct {
	config  *config.Config
	mysql   *mysql.MySql
	elastic *elasticSearch.Elastic
}

func NewListener(cfg *config.Config) {
	listener := &Listener{
		config:  cfg,
		mysql:   mysql.NewMySql(cfg),
		elastic: elasticSearch.NewElastic(cfg),
	}

	//if listener.mongo, err = mongo.NewMongoDB(cfg); err != nil {
	//	panic(err)
	//}
	//
	//if listener.elastic, err = elastic.NewElastic(cfg); err != nil {
	//	panic(err)
	//}
	//for key := range cfg.Collections {
	//	if err = listener.elastic.CheckIndexExisted(key); err != nil {
	//		panic(err)
	//	}
	//}
	//
	//go listener.mongo.CollectionOne.CatchInsertEvent(listener.elastic.Es)
	//go listener.mongo.CollectionOne.CatchUpdateEvent(listener.elastic.Es)
	//go listener.mongo.CollectionOne.CatchDeleteEvent(listener.elastic.Es)

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
