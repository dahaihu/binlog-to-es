package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"binlog-to-es/config"
	"binlog-to-es/position"
	"binlog-to-es/sink"

	"github.com/go-mysql-org/go-mysql/canal"
	log "github.com/sirupsen/logrus"
)

func main() {
	config, err := config.ReadConfig("example/config.yml")
	if err != nil {
		log.Fatal(err)
	}
	canalConfig := canal.NewDefaultConfig()
	canalConfig.Addr = config.Mysql.Addr
	canalConfig.User = config.Mysql.User
	canalConfig.Password = config.Mysql.Password
	canal, err := canal.NewCanal(canalConfig)
	if err != nil {
		log.Fatal(err)
	}
	positionHandler := position.NewPositionManager("example/position.json",
		time.Duration(config.PositionSaveInterval)*time.Millisecond)
	position, err := positionHandler.Load()
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan struct{}, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := <-sigs
		log.Error("exit with sig ", sig)
		cancel()
		canal.Close()
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
