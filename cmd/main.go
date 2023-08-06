package main

import (
	"flag"
	"mysql-event-cacher/cmd/app"
	"mysql-event-cacher/config"
)

var configFlag = flag.String("config", "./config.toml", "configuration toml file path")

func main() {
	flag.Parse()
	cfg := config.NewConfig(*configFlag)
	app.NewListener(cfg)
}
