package mysql

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/inconshreveable/log15"
	"mysql-event-cacher/config"
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

func NewMySql(cfg *config.Config) *MySql {

	mysqlClient := &MySql{
		logger: log15.New("module", "repository/mysql"),
	}
	if db, err := sql.Open(cfg.MySQLConfig.Database, cfg.MySQLConfig.URI); err != nil {
		mysqlClient.logger.Crit("MySql Connection Error", err)
		return nil
	} else {
		db.SetMaxIdleConns(cfg.MySQLConfig.MaxIdleConns)
		db.SetMaxOpenConns(cfg.MySQLConfig.MaxOpenConns)
		db.SetConnMaxLifetime(time.Duration(cfg.MySQLConfig.ConnMaxLifetime) * time.Second)

		if db.Ping(); err != nil {
			mysqlClient.logger.Crit("MySql Connect Ping Error", err)
			return nil
		} else {
			mysqlClient.logger.Info("MySql Connection Success!!, Let's Code!")
			mysqlClient.DB = db
			return mysqlClient
		}
	}
}
