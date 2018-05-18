package metric_drivers

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"encoding/json"
)

type Mysql struct{
	conn *sql.DB
	tableName string
	insertString string
}

func NewMysqlCounter(DSN string, tableName string, dbPool int) *Mysql{
	db, err := sql.Open("mysql", DSN)
	db.SetMaxIdleConns(dbPool)
	db.SetMaxOpenConns(dbPool)
	if err != nil{
		log.Fatal(err)
	}
	m := &Mysql{conn:db, tableName: tableName}
	m.generateInsertStmt()
	return m
}


func (m *Mysql) Send(key string, name string, Points [][2]float64, tags *map[string]string) error{
	j, _ := json.Marshal(tags)
	_, err := m.conn.Exec(m.insertString, key, name, j, m.aggregatePoints(Points))
	if err != nil {
		return err
	}
	return nil
}

func  (m *Mysql) aggregatePoints(Points [][2]float64) float64{
	var c float64
	for _, p := range Points {
		c += p[1]
	}
	return c
}


func  (m *Mysql) generateInsertStmt() {
	m.insertString = "INSERT DELAYED INTO `"+ m.tableName+"` (`key`, `name`, `tags`, `count`) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `count`= `count` + values(`count`)"
}