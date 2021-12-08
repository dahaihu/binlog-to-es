package rule

import (
	"github.com/go-mysql-org/go-mysql/schema"
)

type Rule interface {
	docID(row []interface{}) (string, error)

	SetTableInfo(table *schema.Table)
	MakeESCreateData(row []interface{}) (*ElasticsearchReq, error)
	MakeESUpdateData(oldRow, newRow []interface{}) (*ElasticsearchReq, error)
	MakeESDeleteData(row []interface{}) (*ElasticsearchReq, error)
}
