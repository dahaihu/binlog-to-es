package sink

import (
	"context"
	"fmt"
	"os"

	"binlog-to-es/config"
	"binlog-to-es/rule"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.TraceLevel)
}

type msgType int64

const (
	MsgElasticsearch = 0
	MsgMysqlPosition = 1
)

type msg struct {
	msgType

	Elasticsearch *rule.ElasticsearchReq
	MysqlPosition *MysqlPosition
}

type MysqlPosition struct {
	mysql.Position

	save bool
}

func (p *MysqlPosition) saves() bool {
	return p.save
}

func newMysqlPositionMsg(position mysql.Position, save bool) *msg {
	return &msg{
		msgType: MsgMysqlPosition,
		MysqlPosition: &MysqlPosition{
			Position: position,
			save:     save,
		},
	}
}

func newElasticsearchMsg(req *rule.ElasticsearchReq) *msg {
	return &msg{
		msgType:       MsgElasticsearch,
		Elasticsearch: req,
	}
}

type Handler struct {
	canal.DummyEventHandler
	ctx        context.Context
	tableRules map[string]rule.Rule
	sync       *Sync
	canal      *canal.Canal
}

func NewHandler(cfg *config.Config, sync *Sync, canal *canal.Canal) (
	*Handler, error,
) {
	handler := new(Handler)
	handler.sync = sync
	handler.canal = canal
	handler.tableRules = make(map[string]rule.Rule)
	for _, database := range cfg.DatabaseRules {
		for _, tableRule := range database.TableRules {
			syncRule := rule.NewRule(tableRule)
			ruleKey := handler.ruleKey(database.Database, tableRule.Table)
			handler.tableRules[ruleKey] = syncRule

			if err := handler.setRuleTableInfo(
				database.Database, tableRule.Table,
			); err != nil {
				return nil, err
			}
		}
	}
	return handler, nil
}

func (h *Handler) ruleKey(database, table string) string {
	return fmt.Sprintf("%s_%s", database, table)
}

func (h *Handler) setRuleTableInfo(database, table string) error {
	ruleKey := h.ruleKey(database, table)
	r, ok := h.tableRules[ruleKey]
	if !ok {
		return nil
	}
	tableInfo, err := h.canal.GetTable(database, table)
	if err != nil {
		return err
	}
	r.SetTableInfo(tableInfo)
	return nil
}

func (h *Handler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}
	log.Infof("on rotate event: new position", pos)
	return h.sync.cacheMsg(newMysqlPositionMsg(pos, true))
}

func (h *Handler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	log.Infof("on ddl event: new position", nextPos)
	return h.sync.cacheMsg(newMysqlPositionMsg(nextPos, true))
}

func (h *Handler) OnXID(nextPos mysql.Position) error {
	log.Infof("on xid event: new position", nextPos)
	return h.sync.cacheMsg(newMysqlPositionMsg(nextPos, false))
}

func (h *Handler) OnTableChanged(database, table string) error {
	return h.setRuleTableInfo(database, table)
}

func (h *Handler) OnRow(e *canal.RowsEvent) error {
	ruleKey := h.ruleKey(e.Table.Schema, e.Table.Name)
	r, ok := h.tableRules[ruleKey]
	if !ok {
		return nil
	}
	switch e.Action {
	case canal.UpdateAction:
		for idx := 0; idx < len(e.Rows); idx += 2 {
			esReq, err := r.MakeESUpdateData(e.Rows[idx], e.Rows[idx+1])
			if err != nil {
				log.Fatal("")
			}
			if err := h.sync.cacheMsg(newElasticsearchMsg(esReq)); err != nil {
				return err
			}
		}
	default:
		var (
			esReq *rule.ElasticsearchReq
			err   error
		)
		for idx := 0; idx < len(e.Rows); idx++ {
			switch e.Action {
			case canal.InsertAction:
				esReq, err = r.MakeESCreateData(e.Rows[idx])
			case canal.DeleteAction:
				esReq, err = r.MakeESDeleteData(e.Rows[idx])
			default:
				return fmt.Errorf("invalid mysql %s", e.Action)
			}
			if err != nil {
				return err
			}
			if err := h.sync.cacheMsg(newElasticsearchMsg(esReq)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) String() string {
	return "Handler"
}
