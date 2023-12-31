package mysql

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/inconshreveable/log15"
	"mysql-event-cacher/config"
	"mysql-event-cacher/types"
	"time"
)

type MySql struct {
	DB     *sql.DB
	logger log15.Logger
}

type ISqlContext interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func NewMySql(cfg *config.Config) (*MySql, error) {
	mysqlClient := &MySql{
		logger: log15.New("module", "repository/mysql"),
	}

	mysqlConf := cfg.MySQLConfig

	if db, err := sql.Open(mysqlConf.Database, mysqlConf.Uri); err != nil {
		return nil, err
	} else {
		db.SetMaxIdleConns(mysqlConf.MaxIdleConns)
		db.SetMaxOpenConns(mysqlConf.MaxOpenConns)
		db.SetConnMaxLifetime(time.Duration(mysqlConf.ConnMaxLifetime) * time.Second)

		if db.Ping(); err != nil {
			return nil, err
		} else {
			mysqlClient.DB = db
			return mysqlClient, nil
		}
	}
}

func (m *MySql) GetPosition(ctx context.Context) (*types.Position, error) {

	var pos types.Position

	err := m.DB.QueryRowContext(ctx, "SELECT beforePos FROM eventCacker.position WHERE id = ?;", 1).Scan(
		&pos.Position,
	)

	return &pos, err
}

func (m *MySql) UpdatePosition(ctx context.Context, newPos int64) error {
	_, err := m.DB.ExecContext(ctx, "UPDATE eventCacker.position SET beforePos = ? WHERE id = 1", newPos)
	return err
}
