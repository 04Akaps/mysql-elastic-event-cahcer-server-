package repository

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	r "github.com/go-redis/redis/v7"
	"github.com/inconshreveable/log15"
	"mysql-event-cacher/config"
	"mysql-event-cacher/repository/elasticSearch"
	m "mysql-event-cacher/repository/mysql"
	"mysql-event-cacher/repository/redis"
	"mysql-event-cacher/types"
	"sync"
	"time"
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
	mysql  *m.MySql
	els    *elasticSearch.Elastic
	redis  *redis.RedisClient
	logger log15.Logger

	updateChannel   chan int64
	updatedPosition int64
	mutex           sync.Mutex
}

func NewEventCatch(cfg *config.Config, db *m.MySql, els *elasticSearch.Elastic, redis *redis.RedisClient) error {

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
		return err
	} else {
		eventHandler := &EventCatch{
			mysql:         db,
			els:           els,
			redis:         redis,
			logger:        log15.New("module", "repository/event-catch"),
			updateChannel: make(chan int64),
		}

		var position *types.Position

		if err = eventHandler.redis.Load("position", &position); err != nil {
			if err == r.Nil {
				if position, err = eventHandler.mysql.GetPosition(context.TODO()); err != nil {
					return err
				} else if err = eventHandler.redis.Store("position", &position, 0); err != nil {
					return err
				} else {
					eventHandler.updatedPosition = position.Position
				}
			} else {
				return err
			}
		} else {
			eventHandler.updatedPosition = position.Position
		}

		go eventHandler.updatePosition()

		c.SetEventHandler(eventHandler)

		return c.Run()
	}
}

func (h *EventCatch) updatePosition() {
	// 이벤트가 들어 올 떄 마다 Redis 업데이트 하기
	tenMinuteTicker := time.NewTicker(10 * time.Minute)
	go func() {
		// 10분마다 DB에 현재 Position을 업데이트
		for {
			select {
			case <-tenMinuteTicker.C:
				var position *types.Position
				if err := h.redis.Load("position", &position); err != nil {
					h.logger.Error("Load Current Position while tenMinuteTicker", "error", err)
				} else if err = h.mysql.UpdatePosition(context.TODO(), position.Position); err != nil {
					h.logger.Error("Update Position while tenMinuteTicker", "error", err)
				}

				h.logger.Info("Update Position From Redis To MySql", "info", time.Now().Unix())
			}
		}
	}()

	for {
		select {
		case newPosition := <-h.updateChannel:
			position := types.Position{
				Position: newPosition,
			}
			if err := h.redis.Store("position", position, 0); err != nil {
				h.logger.Error("store New Position To Redis", "error", err)
			}
		}
	}
}

func (h *EventCatch) OnRow(e *canal.RowsEvent) error {
	// 동시 다발적으로 들어오는 경우를 대기
	h.mutex.Lock()
	defer h.mutex.Unlock()
	var err error

	// 간단하게 구성한 원하는 테이블만 이벤트 캐치하는 코드
	if !needToCatchTable(e.Table.Name) {
		return nil
	}

	logPos := e.Header.LogPos

	// Info -> 언제 까지 기록을 했는지를 검증 하고 싶을 떄
	//if uint32(h.updatedPosition) >= logPos {
	//	h.logger.Info("Already Updated Data", "info", e.Header.LogPos)
	//	return nil
	//}

	catchEvent := func(i int, value interface{}) *types.Event {
		var event types.Event
		switch i {
		case 0: // ID
			if event.ID, err = convertToInt64(value); err != nil {
				h.logger.Error("convertToInt64", "error", err)
			}
		case 1: // Name
			if event.Name, err = convertToString(value); err != nil {
				h.logger.Error("convertToString", "error", err)
			}
		case 2: // Age
			if event.Age, err = convertToInt32(value); err != nil {
				h.logger.Error("convertToInt32", "error", err)
			}
		case 3: // CreatedAt
			if event.CreatedAt, err = convertTimeToUnix(value); err != nil {
				h.logger.Error("convertTimeToUnix", "error", err)
			}
		}
		return &types.Event{
			ID:        event.ID,
			Name:      event.Name,
			Age:       event.Age,
			CreatedAt: event.CreatedAt,
		}
	}

	switch e.Action {
	case canal.InsertAction:
		// Handle Insert Event
		for _, row := range e.Rows {
			for i, value := range row {
				data := catchEvent(i, value)
				fmt.Printf("ID: %d, Name: %s, Age: %d, CreatedAt: %d\n", data.ID, data.Name, data.Age, data.CreatedAt)
			}
		}
	case canal.UpdateAction:
		// Handle Update Event
		for _, row := range e.Rows {
			for i, value := range row {
				data := catchEvent(i, value)
				fmt.Printf("ID: %d, Name: %s, Age: %d, CreatedAt: %d\n", data.ID, data.Name, data.Age, data.CreatedAt)
			}
		}
	case canal.DeleteAction:
		// Handle Delete event
		for _, row := range e.Rows {
			for i, value := range row {
				data := catchEvent(i, value)
				fmt.Printf("ID: %d, Name: %s, Age: %d, CreatedAt: %d\n", data.ID, data.Name, data.Age, data.CreatedAt)
			}
		}
	}

	h.updatedPosition = int64(logPos)
	h.updateChannel <- int64(logPos)

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
