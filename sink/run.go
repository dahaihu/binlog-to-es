package sink

import (
	"context"
	"fmt"
	"time"

	"binlog-to-es/rule"
	"binlog-to-es/utils"

	"github.com/go-mysql-org/go-mysql/mysql"
	es "github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type Sync struct {
	ctx             context.Context
	done            chan struct{}
	positionManager Position
	bulkSize        int
	flushInterval   int
	client          *es.Client
	queue           chan *msg
	// position process
	savePosition *mysql.Position
	prePosition  *mysql.Position
	lastPosition *mysql.Position
	requestQueue []*rule.ElasticsearchReq
}

func NewSync(
	ctx context.Context, done chan struct{},
	flushInterval, bulkSize int, position Position) *Sync {
	client, err := es.NewClient()
	if err != nil {
		panic(fmt.Errorf("es client init err %v", err))
	}
	s := &Sync{
		ctx:             ctx,
		done:            done,
		positionManager: position,
		bulkSize:        bulkSize,
		flushInterval:   flushInterval,
		client:          client,
		queue:           make(chan *msg, bulkSize),
		requestQueue:    make([]*rule.ElasticsearchReq, 0, bulkSize*10),
	}
	go s.run()
	return s
}

func (s *Sync) cacheMsg(m *msg) error {
	select {
	case s.queue <- m:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *Sync) flush() error {
	if len(s.requestQueue) == 0 {
		return nil
	}
	req := s.client.Bulk()
	for _, item := range s.requestQueue {
		log.Infof("item is %v", *item)
		var doc es.BulkableRequest
		switch item.Action {
		case utils.ESActionCreate, utils.ESActionUpdate:
			doc = es.NewBulkIndexRequest().Index(item.Index).
				Id(item.ID).OpType(item.Action).Doc(item.Data)
		case utils.ESActionDelete:
			doc = es.NewBulkDeleteRequest().Index(item.Index).
				Id(item.ID)
		default:
			log.Fatal(item.Action)
		}
		req.Add(doc)
	}
	if _, err := req.Do(context.Background()); err != nil {
		// todo report error, and which error should return
		log.Errorf("req es err %err", err)
	}
	s.requestQueue = s.requestQueue[:0]
	return nil
}

func (s *Sync) run() {
	// todo send sig to done
	interval := time.Millisecond * time.Duration(s.flushInterval)
	t := time.NewTicker(interval)
	defer func() { s.done <- struct{}{} }()
	var (
		flush bool
		save  bool
		exit  bool
	)
	for {
		select {
		case <-t.C:
			if len(s.requestQueue) > 0 {
				flush = true
			}
		case item := <-s.queue:
			switch item.msgType {
			case MsgElasticsearch:
				s.requestQueue = append(s.requestQueue, item.Elasticsearch)
				if len(s.requestQueue) >= s.bulkSize {
					flush = true
					// 避免数据flush之后，立刻到达tick的时间，然后继续执行flush
					t.Reset(interval)
				}
			case MsgMysqlPosition:
				if savePosition := item.MysqlPosition.saves(); savePosition {
					flush = true
				} else {
					if needSave := s.positionManager.Update(&item.MysqlPosition.Position); needSave {
						save = true
					}
				}
			}
		case <-s.ctx.Done():
			exit = true
		}
		if exit {
			if err := s.flush(); err != nil {
				log.Error("exist flush err is ", err)
			}
			if err := s.positionManager.Save(); err != nil {
				log.Error("exist save position err", err)
			}
			return
		}
		if flush {
			// every flush save position
			_ = s.flush()
			flush = false
			log.Info("position is ", s.positionManager.Read())
		}
		if save {
			if err := s.positionManager.Save(); err != nil {
				log.Error("save position err", err)
			}
			save = false
		}
	}
}
