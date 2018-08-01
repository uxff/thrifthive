package thrifthive

import (
	"fmt"

	"log"
	"math/rand"
	"time"
	gohive "git.dev.acewill.net/rpc/Gohive" // for thrift
	//gohive "github.com/weigj/go-odbc" // for odbc
	pool "github.com/silenceper/pool"
	//pool "gopkg.in/fatih/pool.v2"

)

// connection pool
var MAX_CONNECTION = 10
var hiveAddr []string
var hivePool pool.Pool
var hiveDbname string

// 连接hive多地址的处理
func openHiveConnection()(interface{}, error) {
	// 随机取一个地址
	randPos := rand.Int()%len(hiveAddr)
	addr := hiveAddr[randPos]

	// gohive 连接
	//conn, err := gohive.Connect(addr, gohive.DefaultOptions)
	tryTimes := 3
	var conn *gohive.Connection
	var err error
	for i := 0; i < tryTimes; i++ {
		conn, err = gohive.Connect(addr, gohive.DefaultOptions)
		if err != nil {
			//log.Println("CONNECT hive errer: " + addr + " error:" + err.Error())
			continue
		}
		break
	}
	if conn == nil {
		panic(fmt.Sprintf("CONNECT hive %v error %d times: %v", addr, tryTimes, err.Error()))
	}

	// 指定了库，就执行选择该库
	if len(hiveDbname) > 0 {
		_, err := conn.Exec("use "+hiveDbname)
		if err != nil {
			log.Println("exec use", hiveDbname, "failed:", err)
		}
	}

	return conn, err
}
// 关闭hive连接的处理
func closeHiveConnection(v interface{}) error {
	return v.(*gohive.Connection).Close()
}

/*
	预先建立连接池
*/
func InitHive(addrs []string, dbname string) (err error) {
	hiveAddr = addrs
	hiveDbname = dbname

	poolConfig := &pool.PoolConfig{
		InitialCap:2,
		MaxCap:MAX_CONNECTION,
		Factory:openHiveConnection,
		Close:closeHiveConnection,
		IdleTimeout:15 * time.Second,
	}

	hivePool, err = pool.NewChannelPool(poolConfig)
	if err != nil {
		log.Println("init hive connection pool error:", err)
	}

	return
}

// 取出一个连接
func GetHiveConnection() (conn *gohive.Connection) {

	v, err := hivePool.Get()
	if err != nil {
		log.Println("get hive connection from pool error:", err)
		return
	}
	// use: v.(*gohive.Connection).Exec("use ods_bi")

	return v.(*gohive.Connection)
}

// 放回一个连接
func PutBackHiveConnection(conn *gohive.Connection) {
	hivePool.Put(conn)
	return
}

func TestHive(queryStr string) {

	conn := GetHiveConnection()
	defer PutBackHiveConnection(conn)

	conn.Exec("use ods_bi")

	results, err := conn.SimpleQuery(queryStr)

	if err != nil {
		log.Println("execute error:", err)
		return
	}

	log.Println("ret=", results)
}
