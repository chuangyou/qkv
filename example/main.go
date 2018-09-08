package main

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

var redisPool *redis.Pool

//redis> SETEX cache_user_id 60 10086
//OK

//redis> GET cache_user_id  # 值
//"10086"

//redis> TTL cache_user_id  # 剩余生存时间
//(integer) 49
type Test struct {
	Username  string `redis:"username"`
	Username1 string `redis:"username1"`
	Age       int    `redis:"age"`
	Sex       int8   `redis:"sex"`
	BirthDay  string `redis:"birthday"`
}

func main() {
	redisPool = newPool("192.168.16.200:8379", "1474741")
	conn := getRedis()
	defer conn.Close()
	log.Println(redis.Int(conn.Do("RPUSH", "mylist7", "one")))
	log.Println(redis.Int(conn.Do("RPUSH", "mylist7", "two")))
	log.Println(redis.Int(conn.Do("RPUSH", "mylist7", "three")))
	log.Println(redis.Int(conn.Do("EXPIRE", "mylist7", 10)))
	log.Println(redis.Int(conn.Do("TTL", "mylist7")))
	time.Sleep(time.Second * 11)
	log.Println(redis.Strings(conn.Do("LRANGE", "mylist7", 0, -1)))
	//	log.Println(redis.Int(conn.Do("LPUSH", "mylist1", "world")))
	//	log.Println(redis.Int(conn.Do("LPUSH", "mylist1", "hello")))
	//	log.Println(redis.String(conn.Do("LSET", "mylist1", 0, "fuck")))
	//	log.Println(redis.Int(conn.Do("LLEN", "mylist1")))
	//	log.Println(redis.Strings(conn.Do("LRANGE", "mylist1", 0, -1)))
	//	log.Println(redis.String(conn.Do("LPOP", "mylist1")))
	//	log.Println(redis.String(conn.Do("LPOP", "mylist1")))
	//	log.Println(redis.Int(conn.Do("LLEN", "mylist1")))
	//	log.Println(conn.Do("HDEL", "zset_test2", "age"))
	//	log.Println(conn.Do("HSET", "zset_test2", "username", "aaaa"))
	//	log.Println(redis.Int(conn.Do("HSTRLEN", "zset_test2", "username")))
	//	log.Println(conn.Do("HMSET", "zset_test2", "sex", 3, "birthday", "1991"))
	//	log.Println(redis.Int(conn.Do("HSETNX", "zset_test2", "username", "1111")))
	//	log.Println(redis.Int(conn.Do("HSETNX", "zset_test2", "username1", "1111")))
	//	log.Println(conn.Do("HINCRBY", "zset_test2", "age", 1))
	//	log.Println(conn.Do("HINCRBY", "zset_test2", "age", 2))
	//	log.Println(conn.Do("HINCRBY", "zset_test2", "age", -3))
	//	//	log.Println(redis.Int(conn.Do("HEXISTS", "zset_test2", "username")))
	//	//	log.Println(redis.Int(conn.Do("HDEL", "zset_test2", "username")))
	//	//	log.Println(redis.Int(conn.Do("HEXISTS", "zset_test2", "username")))
	//	log.Println(redis.Strings(conn.Do("HVALS", "zset_test2")))
	//	log.Println(redis.Strings(conn.Do("HMGET", "zset_test2", "username", "age", "sn")))
	//	log.Println(redis.Int(conn.Do("HLEN", "zset_test2")))
	//	//	log.Println(redis.Int(conn.Do("DEL", "zset_test2")))
	//	log.Println(redis.Int(conn.Do("EXPIRE", "zset_test2", 10)))
	//	log.Println(redis.Int(conn.Do("TTL", "zset_test2")))
	//	time.Sleep(time.Second * 11)
	//	v, _ := redis.Values(conn.Do("HGETALL", "zset_test2"))
	//	testInfo := Test{}
	//	err := redis.ScanStruct(v, &testInfo)
	//	log.Println(err, testInfo)
	//	log.Println(conn.Do("ZINCRBY", "zset_test2", 2, "b"))
	//	log.Println(conn.Do("ZADD", "zset_test2", 456456, "c"))
	//	log.Println(redis.Int(conn.Do("EXPIRE", "zset_test2", 10)))
	//	log.Println(redis.Strings(conn.Do("ZRANGE", "zset_test2", 0, -1, "WITHSCORES")))
	//	time.Sleep(time.Second * 11)
	//	log.Println(redis.Strings(conn.Do("ZRANGE", "zset_test2", 0, -1, "WITHSCORES")))

}
func getRedis() redis.Conn {
	return redisPool.Get()
}
func newPool(host, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:   80,
		MaxActive: 256, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					return nil, err
				}
			}
			return c, err
		},
	}
}
