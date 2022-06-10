package position

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type defaultPosition struct {
	saveInterval time.Duration
	lastSaveTime time.Time
	path         string
	position     *mysql.Position
}

func NewPositionManager(path string, flushInterval time.Duration) Position {
	if flushInterval == 0 {
		flushInterval = 200 * time.Millisecond
	}
	return &defaultPosition{
		path: path,
		position: &mysql.Position{
			Name: "",
			Pos:  0,
		},
		// save every 200ms
		saveInterval: flushInterval,
	}
}

func (p *defaultPosition) Load() (*mysql.Position, error) {
	file, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	pos := new(mysql.Position)
	if err := json.Unmarshal(content, pos); err != nil {
		return nil, err
	}
	p.position = pos
	p.lastSaveTime = time.Now()
	return pos, nil
}

func (p *defaultPosition) Update(pos *mysql.Position) (shouldSave bool) {
	p.position = pos
	return time.Now().After(p.lastSaveTime.Add(p.saveInterval))
}

func (p *defaultPosition) Save() error {
	content, err := json.Marshal(p.position)
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(p.path, content, 0644); err != nil {
		return err
	}
	p.lastSaveTime = time.Now()
	return nil
}

func (p *defaultPosition) Read() *mysql.Position {
	return p.position
}

func (p *defaultPosition) String() string {
	return fmt.Sprintf("binlog file: %s, position: %d;",
		p.position.Name, p.position.Pos)
}
