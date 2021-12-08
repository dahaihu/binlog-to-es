package rule

import (

	"encoding/json"
	"fmt"
	"reflect"

	"binlog-to-es/config"
	"binlog-to-es/utils"

	"github.com/go-mysql-org/go-mysql/schema"
)

type Type int

const (
	Normal Type = iota
	Nested
)

type ElasticsearchReq struct {
	Action string
	Index  string
	ID     string
	Data   string
}


type baseRule struct {
	config.TableRule

	FieldMapping    map[string]string
	SyncedFieldsSet utils.Set
	Columns         []string
	TableInfo       *schema.Table
}

func NewRule(config config.TableRule) Rule {
	rule := baseRule{TableRule: config}
	rule.SyncedFieldsSet = utils.NewSet(rule.SyncedFields)
	rule.FieldMapping = make(map[string]string)
	for _, fieldMap := range rule.FieldMappings {
		rule.FieldMapping[fieldMap.MysqlField] = fieldMap.ESField
	}
	if config.Nested != nil {
		return &nestedRule{
			baseRule:    rule,
			PrimaryKey:  config.Nested.PrimaryKey,
			NestedField: config.Nested.NestedField,
		}
	}
	return &rule
}

func (r *baseRule) Marshal(data map[string]interface{}) string {
	rawData, _ := json.Marshal(data)
	return string(rawData)
}

func (r *baseRule) SetTableInfo(tableInfo *schema.Table) {
	r.TableInfo = tableInfo
	columns := make([]string, 0, len(r.TableInfo.Columns))
	for _, column := range r.TableInfo.Columns {
		columns = append(columns, column.Name)
	}
	r.Columns = columns
}

func (r *baseRule) makeCreateDoc(row []interface{}) (
	string, map[string]interface{}, error,
) {
	docID, err := r.docID(row)
	if err != nil {
		return "", nil, err
	}
	doc := r.makeCreateData(row)
	return docID, doc, nil
}

func (r *baseRule) docID(row []interface{}) (string, error) {
	docIDInterface, err := r.TableInfo.GetColumnValue(r.DocID, row)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", docIDInterface), err
}

func (r *baseRule) filter(column string) bool {
	return !r.SyncedFieldsSet.Contains(column)
}

func (r *baseRule) esField(column string) string {
	fieldMap, ok := r.FieldMapping[column]
	if ok {
		return fieldMap
	}
	return column
}

func (r *baseRule) makeCreateData(row []interface{}) map[string]interface{} {
	data := make(map[string]interface{}, len(r.Columns))
	for idx, column := range r.Columns {
		if r.filter(column) {
			continue
		}
		data[r.esField(column)] = row[idx]
	}
	return data
}

func (r *baseRule) makeUpdateData(oldRow, newRow []interface{}) (
	map[string]interface{},
) {
	data := make(map[string]interface{})
	for idx, _ := range oldRow {
		column := r.Columns[idx]
		if r.filter(column) {
			continue
		}
		oldValue := oldRow[idx]
		newValue := newRow[idx]
		if reflect.DeepEqual(oldValue, newValue) {
			continue
		}
		data[r.esField(column)] = newValue
	}
	return data
}
