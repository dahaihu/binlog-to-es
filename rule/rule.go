package rule

import (
	"binlog-to-es/utils"
)

func (r *baseRule) MakeESCreateData(row []interface{}) (
	*ElasticsearchReq, error,
) {
	id, data, err := r.makeCreateDoc(row)
	if err != nil {
		return nil, err
	}
	return &ElasticsearchReq{
		Action: utils.ESActionCreate,
		Index:  r.Index,
		ID:     id,
		Data:   r.Marshal(data),
	}, nil
}

func (r *baseRule) MakeESUpdateData(
	oldRow, newRow []interface{},
) (*ElasticsearchReq, error) {
	id, err := r.docID(newRow)
	if err != nil {
		return nil, err
	}
	data := r.makeUpdateData(oldRow, newRow)
	return &ElasticsearchReq{
		Action: utils.ESActionUpdate,
		Index:  r.Index,
		ID:     id,
		Data:   r.Marshal(data),
	}, nil
}

func (r *baseRule) MakeESDeleteData(row []interface{}) (
	*ElasticsearchReq, error,
) {
	id, err := r.docID(row)
	if err != nil {
		return nil, err
	}
	return &ElasticsearchReq{
		Action: utils.ESActionDelete,
		Index:  r.Index,
		ID:     id,
		Data:   "",
	}, nil
}
