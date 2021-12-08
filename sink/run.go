package sink

import (
	"context"
	"time"

	"binlog-to-es/rule"
	. "binlog-to-es/utils"

	es "github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type Sync struct {
	PositionManager Position
	BulkSize        int64
	FlushInterval   int64
	Client          *es.Client
	Queue           chan *msg
}

func NewSync(flushInterval, bulkSize int64, position Position) *Sync {
	client, _ := es.NewClient()
	s := &Sync{
		PositionManager: position,
		BulkSize:        bulkSize,
		FlushInterval:   flushInterval,
		Client:          client,
		Queue:           make(chan *msg, bulkSize),
	}
	go s.run()
	return s
}

func (s *Sync) run() {
	interval := time.Millisecond * time.Duration(s.FlushInterval)
	t := time.NewTicker(interval)
	syncQueue := make([]*rule.ElasticsearchReq, 0, s.BulkSize*10)
	var flush bool
	for {
		select {
		case <-t.C:
			if len(syncQueue) > 0 {
				flush = true
			}
		case item := <-s.Queue:
			switch item.msgType {
			case MsgElasticsearch:
				syncQueue = append(syncQueue, item.Elasticsearch)
				if len(syncQueue) >= 100 {
					flush = true
					// 避免数据flush之后，立刻到达tick的时间，然后继续执行flush
					t.Reset(interval)
				}
			case MsgMysqlPosition:
				s.PositionManager.Update(item.MysqlPosition)
			}
		}
		if flush {
			req := s.Client.Bulk()
			for _, item := range syncQueue {
				log.Infof("item is %v", *item)
				var doc es.BulkableRequest
				switch item.Action {
				case ESActionCreate, ESActionUpdate:
					doc = es.NewBulkIndexRequest().Index(item.Index).
						Id(item.ID).OpType(item.Action).Doc(item.Data)
				case ESActionDelete:
					doc = es.NewBulkDeleteRequest().Index(item.Index).
						Id(item.ID)
				default:
					log.Fatal(item.Action)
				}
				req.Add(doc)
			}
			if resp, err := req.Do(context.Background()); err != nil {
				log.Errorf("req es err %err", err)
			} else {
				for _, item := range resp.Failed() {
					log.Infof("failed %v", *item)
					log.Infof("failed reason is %v", *item.Error)
				}
				log.Infof("took %d", resp.Took)
			}
			log.Infof("flush %d docs", len(syncQueue))
			flush = false
			syncQueue = syncQueue[0:0]
			log.Info("position is ", s.PositionManager.Read())
		}
	}
}
