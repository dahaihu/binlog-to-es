package sink

import (
	"context"
	"fmt"
	"time"

	"binlog-to-es/metric"
	"binlog-to-es/position"
	"binlog-to-es/rule"
	"binlog-to-es/utils"

	es "github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type Sync struct {
	ctx             context.Context
	done            chan struct{}
	positionManager position.Position
	bulkSize        int
	flushInterval   int
	client          *es.Client
	queue           chan *msg
	requestQueue    []*rule.ElasticsearchReq
}

func NewSync(
	ctx context.Context, done chan struct{},
	flushInterval, bulkSize int, position position.Position,
) *Sync {
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
		metric.Queued.Inc()
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
		case utils.ESActionCreate:
			metric.IndexCreated.Inc()
			doc = es.NewBulkIndexRequest().Index(item.Index).
				Id(item.ID).OpType(item.Action).Doc(item.Data)
		case utils.ESActionUpdate:
			metric.IndexUpdated.Inc()
			upData := es.NewBulkUpdateRequest().Index(item.Index).Id(item.ID)
			if item.Script != "" {
				script := es.NewScript(item.Script)
				script.Params(item.Data)
				upData.Script(script)
			} else {
				upData.Doc(item.Data)
			}
			doc = upData
		case utils.ESActionDelete:
			metric.IndexDeleted.Inc()
			doc = es.NewBulkDeleteRequest().Index(item.Index).Id(item.ID)
		default:
			log.Fatal(item.Action)
		}
		reqBody, _ := doc.Source()
		log.Infof("es request is %s", reqBody)
		req.Add(doc)
	}
	startTime := time.Now()
	actionNumber := req.NumberOfActions()
	defer func() {
		metric.FlushTime.Observe(time.Since(startTime).Seconds())
		log.Infof("flush %d time is %f", actionNumber,
			time.Since(startTime).Seconds())
	}()
	if _, err := req.Do(context.Background()); err != nil {
		// todo report error, and which error should return
		log.Errorf("req es err %err", err)
	}
	s.requestQueue = s.requestQueue[:0]
	return nil
}

func (s *Sync) run() {
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
			if err := s.flush(); err != nil {
				log.Error("flush err is ", err)
			}
			log.Info("position is ", s.positionManager.Read())
			flush = false
		}
		if save {
			if err := s.positionManager.Save(); err != nil {
				log.Error("save position err", err)
			}
			log.Info("save position is ", s.positionManager.Read())
			save = false
		}
	}
}
