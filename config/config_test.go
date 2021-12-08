package config

import (
	"fmt"
	"testing"
)

func TestReadConfig(t *testing.T) {
	config, err := ReadConfig("./../config.yml")
	fmt.Println(config, err)
	fmt.Println(config.Mysql)
	fmt.Println(config.Elasticsearch)
	for _, databaseRules := range config.DatabaseRules {
		for _, rule := range databaseRules.TableRules {
			fmt.Println(databaseRules.Database, rule)
		}
	}
}
