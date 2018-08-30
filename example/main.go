package main

import (
	"log"
	//"time"

	"github.com/garyburd/redigo/redis"
)

var redisPool *redis.Pool

//redis> SETEX cache_user_id 60 10086
//OK

//redis> GET cache_user_id  # 值
//"10086"

//redis> TTL cache_user_id  # 剩余生存时间
//(integer) 49
func main() {
	redisPool = newPool("192.168.16.200:8379", "1474741")
	conn := getRedis()
	defer conn.Close()
	log.Println(conn.Do("ZADD", "rank3", 1, "a"))
	log.Println(conn.Do("ZADD", "rank3", 2, "b"))
	log.Println(conn.Do("ZADD", "rank3", 2, "c"))
	log.Println(conn.Do("ZINCRBY", "rank3", 2, "c"))
	log.Println(conn.Do("ZINCRBY", "rank3", 2, "c"))
	log.Println(conn.Do("ZINCRBY", "rank3", 200, "c"))
	log.Println(conn.Do("ZINCRBY", "rank3", -100, "c"))
	log.Println(redis.Strings(conn.Do("ZRANGE", "rank3", 0, -1, "WITHSCORES")))
	log.Println(redis.Int(conn.Do("ZLEXCOUNT", "rank3", "-", "+")))
	log.Println(redis.Int(conn.Do("ZCARD", "rank3")))
	log.Println(redis.Int(conn.Do("ZCOUNT", "rank3", "1", "2")))

	//	log.Println(conn.Do("SADD", "key1", "b"))
	//	log.Println(conn.Do("SADD", "key1", "c"))
	//	log.Println(conn.Do("SADD", "key1", "f"))
	//	log.Println(conn.Do("SADD", "key2", "c"))
	//	log.Println(conn.Do("SADD", "key2", "d"))
	//	log.Println(conn.Do("SADD", "key2", "e"))
	//	log.Println(conn.Do("SADD", "key2", "f"))
	//	log.Println(conn.Do("SINTER", "key1", "key2"))
	//	log.Println(conn.Do("SINTERSTORE", "key", "key1", "key2"))
	//	log.Println(redis.Strings(conn.Do("SMEMBERS", "key")))
	//	log.Println(conn.Do("SREM", "key", "c"))
	//	log.Println(conn.Do("SREM", "key", "f"))
	//	log.Println(redis.Strings(conn.Do("SMEMBERS", "keytest2")))
	//	log.Println(redis.Strings(conn.Do("SMEMBERS", "key2")))
	//	log.Println(conn.Do("SADD", "key2", "e"))
	//	log.Println(conn.Do("SADD", "key2", "f"))
	//	log.Println(redis.Strings(conn.Do("SMEMBERS", "key2")))
	//	log.Println(conn.Do("SPEXPIREAT", "key2", 1635610846000))
	//	log.Println(conn.Do("SPTTL", "key2"))
	//	time.Sleep(time.Second * 11)
	//	log.Println(redis.Strings(conn.Do("SMEMBERS", "key2")))
	//	time.Sleep(time.Second * 11)
	//	conn.Do("SDEL", "key2")
	//	log.Println(redis.Strings(conn.Do("SMEMBERS", "key2")))
	//	log.Println(conn.Do("EXPIRE", "key", "10"))
	//	log.Println(conn.Do("TTL", "key"))
	//	log.Println(conn.Do("SET", "cache_user_id", 10))
	//	log.Println(conn.Do("EXPIRE", "cache_user_id", 10))
	//	log.Println(conn.Do("SET", "cache_user_id1", 10))
	//	log.Println(conn.Do("EXPIRE", "cache_user_id1", 10))
	//	log.Println(conn.Do("SET", "cache_user_id2", 10))
	//	log.Println(conn.Do("EXPIRE", "cache_user_id2", 10))
	//	log.Println(redis.Int(conn.Do("TTL", "cache_user_id")))
	//	time.Sleep(time.Second * 20)
	//	log.Println(redis.Int(conn.Do("GET", "cache_user_id")))
	//	log.Println(redis.Int(conn.Do("TTL", "cache_user_id")))
	//	log.Println(redis.Int(conn.Do("GET", "cache_user_id1")))
	//	log.Println(redis.Int(conn.Do("TTL", "cache_user_id1")))
	//	log.Println(redis.Int(conn.Do("GET", "cache_user_id2")))
	//	log.Println(redis.Int(conn.Do("TTL", "cache_user_id2")))
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
