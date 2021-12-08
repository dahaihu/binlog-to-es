package main

import (
	"binlog-to-es/config"
	"binlog-to-es/sink"

	"github.com/go-mysql-org/go-mysql/canal"
	log "github.com/sirupsen/logrus"
)

func main() {
	canalConfig := canal.NewDefaultConfig()
	canalConfig.Addr = "127.0.0.1:3306"
	canalConfig.User = "root"
	canalConfig.Password = "669193"

	config, err := config.ReadConfig("./config.yml")
	if err != nil {
		log.Fatal(err)
	}
	canal, err := canal.NewCanal(canalConfig)
	if err != nil {
		log.Fatal(err)
	}

	positionHandler := sink.NewPositionManager("position.json")
	position, err := positionHandler.Load()
	if err != nil {
		log.Fatal(err)
	}
	handler, err := sink.NewHandler(
		config,
		sink.NewSync(
			config.Elasticsearch.FlushInterval,
			config.Elasticsearch.BulkSize,
			positionHandler,
		),
		canal,
	)
	if err != nil {
		log.Fatal(err)
	}
	// Register a handler to handle RowsEvent
	canal.SetEventHandler(handler)

	// Start canal
	canal.RunFrom(*position)
}
