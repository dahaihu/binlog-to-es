package sink

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"io/ioutil"
	"os"
)

type Position interface {
	Load() (*mysql.Position, error)
	Read() *mysql.Position
	Update(*mysql.Position) error
	Save() error
}

type defaultPosition struct {
	Path string

	Position *mysql.Position
}

func NewPositionManager(path string) Position {
	return &defaultPosition{
		Path: path,
		Position: &mysql.Position{
			Name: "",
			Pos:  0,
		},
	}
}

func (p *defaultPosition) Load() (*mysql.Position, error) {
	file, err := os.Open(p.Path)
	if err != nil {
		panic(err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var pos mysql.Position
	if err := json.Unmarshal(content, &pos); err != nil {
		return nil, err
	}
	return &pos, nil
}

func (p *defaultPosition) Update(pos *mysql.Position) error {
	p.Position = pos
	return nil
}

func (p *defaultPosition) Save() error {
	content, err := json.Marshal(p.Position)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(p.Path, content, 0644); err != nil {
		return err
	}
	return nil
}

func (p *defaultPosition) Read() *mysql.Position {
	return p.Position
}

func (p *defaultPosition) String() string {
	return fmt.Sprintf("binlog file: %s, position: %d;",
		p.Position.Name, p.Position.Pos)
}
