/*
   connect thrift, use odbc protocol
*/
package main

import (
	"flag"
	"fmt"

	"math/rand"
	"time"

	"log"

	//gohive "github.com/uxff/thrifthive/gohive" // for thrift
	//gohive "github.com/weigj/go-odbc" // for odbc
	pool "github.com/silenceper/pool"
	gohive "github.com/uxff/go-odbc"
	//pool "gopkg.in/fatih/pool.v2"
)

// connection pool
var MAX_CONNECTION = 10
var hiveAddr []string
var hivePool pool.Pool
var hiveDbname string

// 连接hive多地址的处理
func openHiveConnection() (interface{}, error) {

	var addr string

	// gohive 连接
	//conn, err := gohive.Connect(addr, gohive.DefaultOptions)

	// 多次尝试
	tryTimes := 3
	var conn *gohive.Connection
	var err error
	for i := 0; i < tryTimes; i++ {
		err = nil
		// 随机取一个地址
		randPos := rand.Int() % len(hiveAddr)
		addr = hiveAddr[randPos]
		// err 可能不是nil,但是conn有值
		conn, err = gohive.Connect("DSN=" + addr)
		if conn != nil {
			break
		}
		log.Printf("CONNECT hive errer: " + addr + " error:" + err.Error())
	}

	if conn == nil {
		panic(fmt.Sprintf("CONNECT hive %v error %d times: %v", addr, tryTimes, err.Error()))
	} else {
		err = nil
	}

	//select db
	if len(hiveDbname) > 0 {
		_, err := conn.ExecDirect("use " + hiveDbname)
		if err != nil {
			log.Printf("exec use", hiveDbname, "failed:", err)
		}
	}

	return conn, err
}

// close hive
func closeHiveConnection(v interface{}) error {
	return v.(*gohive.Connection).Close()
}

/*
	prepare hive connect pool
*/
func InitHive(addrs []string, dbname string) (err error) {
	hiveAddr = addrs
	hiveDbname = dbname

	poolConfig := &pool.PoolConfig{
		InitialCap:  2,
		MaxCap:      MAX_CONNECTION,
		Factory:     openHiveConnection,
		Close:       closeHiveConnection,
		IdleTimeout: 15 * time.Second,
	}

	hivePool, err = pool.NewChannelPool(poolConfig)
	if err != nil {
		log.Printf("init hive connection pool error:", err)
		return err
	}

	return
}

//
func GetHiveConnection() (conn *gohive.Connection) {

	v, err := hivePool.Get()
	if err != nil {
		log.Printf("get hive connection from pool error:", err)
		return
	}
	// use: v.(*gohive.Connection).Exec("use ods_bi")

	return v.(*gohive.Connection)
}

//
func PutBackHiveConnection(conn *gohive.Connection) {
	hivePool.Put(conn)
	return
}

func TestHive(queryStr string) {

	conn := GetHiveConnection()
	defer PutBackHiveConnection(conn)

	conn.ExecDirect("use ods_bi")

	stmt, err := conn.Prepare(queryStr)

	if err != nil {
		log.Printf("execute error:", err)
		return
	}

	rets, err := stmt.FetchAll()

	log.Printf("ret=", rets)
}

var (
	defaultHiveAddr   = "101.201.57.40:50000"
	defaultHiveDbName = "the_hive_db"
)

func main() {
	hiveAddr := flag.String("hiveaddr", defaultHiveAddr, "addr of hive")
	dbname := flag.String("hivedb", defaultHiveDbName, "hive database name you use")
	flag.Parse()

	InitHive([]string{*hiveAddr}, *dbname)
	queryStr := "show tables"
	TestHive(queryStr)

	defer hivePool.Release()
}
