package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	positionHandler := sink.NewPositionManager("position.json",
		time.Duration(config.Elasticsearch.FlushInterval))
	position, err := positionHandler.Load()
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan struct{}, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	cancelCtx, cancel := context.WithCancel(context.Background())
	go func () {
		select {
		case sig := <- sigs:
			log.Error("get sig ", sig)
			cancel()
			canal.Close()
		}
	}()
	handler, err := sink.NewHandler(
		config,
		sink.NewSync(
			cancelCtx,
			done,
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
	if err := canal.RunFrom(*position); err != nil {
		log.Error("canal run err", err)
	}
	<-done
}
