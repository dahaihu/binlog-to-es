package rule

import (
	"fmt"

	"binlog-to-es/utils"
)

type nestedRule struct {
	baseRule

	// nested field to distinct from other resource
	PrimaryKey  string
	NestedField string
}

func (r *nestedRule) MakeCreateData(row []interface{}) (
	*ElasticsearchReq, error,
) {
	id, data, err := r.makeCreateDoc(row)
	if err != nil {
		return nil, err
	}
	script := r.makeNestedCreateScript()
	return &ElasticsearchReq{
		Action: utils.ESActionUpdate,
		Index:  r.Index,
		ID:     id,
		Data:   data,
		Script: script,
	}, nil
}

func (r *nestedRule) MakeUpdateData(
	oldRow, newRow []interface{},
) (*ElasticsearchReq, error) {
	return r.MakeCreateData(newRow)
}

func (r *nestedRule) MakeDeleteData(row []interface{}) (
	*ElasticsearchReq, error,
) {
	id, data, err := r.makeCreateDoc(row)
	if err != nil {
		return nil, err
	}
	script := r.makeNestedDeleteScript()

	return &ElasticsearchReq{
		Action: utils.ESActionUpdate,
		Index:  r.Index,
		ID:     id,
		Data:   data,
		Script: script,
	}, nil
}

func (r *nestedRule) makeNestedCreateScript() string {
	return fmt.Sprintf(
		`if (ctx._source.%[1]s == null) {
							ctx._source.%[1]s = new ArrayList();
		}
		ctx._source.%[1]s.removeIf(item -> item.%[2]s == params.%[2]s);
		ctx._source.%[1]s.add(params)`,
		r.Nested.NestedField, r.Nested.PrimaryKey,
	)
}

//// make delete nested filed data
func (r *nestedRule) makeNestedDeleteScript() string {
	return fmt.Sprintf(
		`ctx._source.%s.removeIf(item -> item.%s == params.%s)`,
		r.Nested.NestedField, r.Nested.PrimaryKey, r.Nested.PrimaryKey,
	)
}
