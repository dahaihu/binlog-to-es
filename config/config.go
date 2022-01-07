package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Mysql struct {
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type Nested struct {
	// nested field to distinct from other resource
	PrimaryKey  string `yaml:"primary_key"`
	NestedField string `yaml:"nested_field"`
}

type Elasticsearch struct {
	Host          string `yaml:"host"`
	Port          int64  `yaml:"port"`
	BulkSize      int    `yaml:"bulk_size"`
	FlushInterval int    `yaml:"flush_interval"`
}

type FieldMapping struct {
	MysqlField string `yaml:"mysql_field"`
	ESField    string `yaml:"es_field"`
}

type TableRule struct {
	Table         string         `yaml:"table"`
	Index         string         `yaml:"index"`
	DocID         string         `yaml:"doc_id"`
	Nested        *Nested        `yaml:"nested"`
	FieldMappings []FieldMapping `yaml:"field_mappings"`
	SyncedFields  []string       `yaml:"sync_fields"`
}

type DatabaseRule struct {
	Database   string      `yaml:"database"`
	TableRules []TableRule `yaml:"table_rules"`
}

type Config struct {
	Mysql         Mysql          `yaml:"mysql"`
	Elasticsearch Elasticsearch  `yaml:"elasticsearch"`
	DatabaseRules []DatabaseRule `yaml:"database_rules"`
}

func ReadConfig(path string) (*Config, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	config := new(Config)
	err = yaml.Unmarshal(content, config)
	if err != nil {
		return nil, err
	}
	return config, err
}
