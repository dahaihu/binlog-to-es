package position

import "github.com/go-mysql-org/go-mysql/mysql"

type Position interface {
	Load() (*mysql.Position, error)
	Read() *mysql.Position
	Update(*mysql.Position) (shouldSave bool)
	Save() error
}

