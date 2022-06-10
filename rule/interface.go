package rule

import (
	"github.com/go-mysql-org/go-mysql/schema"
)

type Rule interface {
	docID(row []interface{}) (string, error)

	SetTableInfo(table *schema.Table)
	MakeCreateData(row []interface{}) (*ElasticsearchReq, error)
	MakeUpdateData(oldRow, newRow []interface{}) (*ElasticsearchReq, error)
	MakeDeleteData(row []interface{}) (*ElasticsearchReq, error)
}
