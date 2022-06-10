package rule

import (
	"binlog-to-es/utils"
)

func (r *baseRule) MakeCreateData(row []interface{}) (
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
		Data:   data,
	}, nil
}

func (r *baseRule) MakeUpdateData(
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
		Data:   data,
	}, nil
}

func (r *baseRule) MakeDeleteData(row []interface{}) (
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
	}, nil
}
