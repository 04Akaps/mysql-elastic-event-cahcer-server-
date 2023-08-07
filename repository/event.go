package repository

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/inconshreveable/log15"
	"mysql-event-cacher/config"
	"mysql-event-cacher/repository/elasticSearch"
	m "mysql-event-cacher/repository/mysql"
	"mysql-event-cacher/types"
)

func needToCatchTable(table string) bool {
	return tableMap[table]
}

func addTableMap(tables []string) {
	for _, table := range tables {
		if ok := tableMap[table]; !ok {
			tableMap[table] = true
		} else {
			panic("Already Existed Table")
		}
	}
}

var tableMap map[string]bool

type EventCatch struct {
	mysql *m.MySql
	els   *elasticSearch.Elastic

	logger          log15.Logger
	updatedPosition int64
}

func NewEventCatch(cfg *config.Config, db *m.MySql, els *elasticSearch.Elastic) {

	tableMap = make(map[string]bool)

	tableList := []string{"event"}

	addTableMap(tableList)

	conf := &canal.Config{
		ServerID:  1,
		Addr:      cfg.MySQLConfig.Addr,
		User:      cfg.MySQLConfig.User,
		Password:  cfg.MySQLConfig.Password,
		Charset:   mysql.DEFAULT_CHARSET,
		Flavor:    "mysql",
		ParseTime: true,
	}

	if c, err := canal.NewCanal(conf); err != nil {
		panic(err)
	} else {
		eventHandler := &EventCatch{
			mysql:  db,
			els:    els,
			logger: log15.New("module", "repository/event-catch"),
		}

		eventHandler.updatedPosition, err = eventHandler.mysql.GetPosition(context.TODO())
		if err != nil {
			panic(err)
		}

		c.SetEventHandler(eventHandler)

		err = c.Run()
		if err != nil {
			panic(err)
		}
	}

}

func (h *EventCatch) OnRow(e *canal.RowsEvent) error {
	var event types.Event
	var err error

	// 간단하게 구성한 원하는 테이블만 이벤트 캐치하는 코드
	if !needToCatchTable(e.Table.Name) {
		return nil
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			for i, value := range row {
				switch i {
				case 0: // ID
					if event.ID, err = convertToInt64(value); err != nil {
						h.logger.Error("convertToInt64", "error", err)
						continue
					}
				case 1: // Name
					if event.Name, err = convertToString(value); err != nil {
						h.logger.Error("convertToString", "error", err)
						continue
					}
				case 2: // Age
					if event.Age, err = convertToInt32(value); err != nil {
						h.logger.Error("convertToInt32", "error", err)
						continue
					}
				case 3: // CreatedAt
					if event.CreatedAt, err = convertTimeToUnix(value); err != nil {
						h.logger.Error("convertTimeToUnix", "error", err)
						continue
					}
				}
			}
			fmt.Printf("ID: %d, Name: %s, Age: %d, CreatedAt: %d\n", event.ID, event.Name, event.Age, event.CreatedAt)
		}
	case canal.UpdateAction:

		for _, row := range e.Rows {
			for i, value := range row {
				switch i {
				case 0: // ID
					if event.ID, err = convertToInt64(value); err != nil {
						h.logger.Error("convertToInt64", "error", err)
						break
					}
				case 1: // Name
					if event.Name, err = convertToString(value); err != nil {
						h.logger.Error("convertToString", "error", err)
						break
					}
				case 2: // Age
					if event.Age, err = convertToInt32(value); err != nil {
						h.logger.Error("convertToInt32", "error", err)
						break
					}
				case 3: // CreatedAt
					if event.CreatedAt, err = convertTimeToUnix(value); err != nil {
						h.logger.Error("convertTimeToUnix", "error", err)
						continue
					}
				}
			}
			fmt.Printf("UPdated!!!! ID: %d, Name: %s, Age: %d, CreatedAt: %d\n", event.ID, event.Name, event.Age, event.CreatedAt)
		}
		// Handle Update event
	case canal.DeleteAction:
		// Handle Delete event
	}

	return nil
}

func (h *EventCatch) String() string {
	panic("implement me")
}

func (h *EventCatch) OnRotate(header *replication.EventHeader, r *replication.RotateEvent) error {
	return nil
}

func (h *EventCatch) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	return nil
}

func (h *EventCatch) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (h *EventCatch) OnGTID(*replication.EventHeader, mysql.GTIDSet) error {
	return nil
}

func (h *EventCatch) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *EventCatch) OnXID(*replication.EventHeader, mysql.Position) error {
	return nil
}

func (h *EventCatch) OnUnmarshal(data []byte) (interface{}, error) {
	return nil, nil
}

func (h *EventCatch) OnRawEvent(event *replication.BinlogEvent) error {
	return nil
}
