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

func (r *nestedRule) MakeESCreateData(row []interface{}) (
	*ElasticsearchReq, error,
) {
	id, data, err := r.makeCreateDoc(row)
	if err != nil {
		return nil, err
	}
	data = r.makeNestedUpdateData(data)
	return &ElasticsearchReq{
		Action: utils.ESActionUpdate,
		Index:  r.Index,
		ID:     id,
		Data:   r.Marshal(data),
	}, nil
}

func (r *nestedRule) MakeESUpdateData(
	oldRow, newRow []interface{},
) (*ElasticsearchReq, error) {
	return r.MakeESCreateData(newRow)
}

func (r *nestedRule) MakeESDeleteData(row []interface{}) (
	*ElasticsearchReq, error,
) {
	id, data, err := r.makeCreateDoc(row)
	if err != nil {
		return nil, err
	}
	data = r.makeNestedDeleteData(data)

	return &ElasticsearchReq{
		Action: utils.ESActionUpdate,
		Index:  r.Index,
		ID:     id,
		Data:   r.Marshal(data),
	}, nil
}

func (r *nestedRule) makeNestedUpdateData(
	data map[string]interface{},
) map[string]interface{} {
	return map[string]interface{}{
		"script": map[string]interface{}{
			"source": fmt.Sprintf(
				`if (ctx._source.%[1]s == null) {
							ctx._source.%[1]s = new ArrayList();
						}
						ctx._source.%[1]s.removeIf(item -> item.%[2]s == params.%[2]s);
						ctx._source.%[1]s.add(params)`,
				r.Nested.NestedField, r.Nested.PrimaryKey,
			),
			"params": data,
		},
	}
}

//// make delete nested filed data
func (r *nestedRule) makeNestedDeleteData(data map[string]interface{}) (
	map[string]interface{},
) {
	return map[string]interface{}{
		"script": map[string]interface{}{
			"source": fmt.Sprintf(
				`ctx._source.%s.removeIf(item -> item.%s == params.%s)`,
				r.Nested.NestedField, r.Nested.PrimaryKey, r.Nested.PrimaryKey,
			),
			"params": data,
		},
	}
}
