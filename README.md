# QKV - 分布式NoSql数据库

QKV是基于TiKV打造的一款兼容redis协议的分布式NoSql数据库，支持string、set、zset、list、hash数据类型操作，使用Go语言编写，具有高可用、高性能、高扩展性。

## 特性

- 兼容redis协议
- 采用TiKV，支持分布式事务
- 大数据量，支持水平扩展
- 高性能，在较高QPS的同时能够保证比较低的延时
- 高可靠，数据持久化存储，采用raft协议实现一致性，防止数据丢失
- 高可用，集群内部分节点失效不影响集群使用
- 易运维，可以在不停机的情况下进行数据迁移与集群扩容
- QKV只做协议转发与数据计算操作，TiKV做存储

## 架构图如下
## 安装部署

PD以及TiKV部署参考如下（这里我们只需用到PD和TiKV）：
- https://www.pingcap.com/docs-cn/

QKV（生成二进制文件）:
- go get -u github.com/chuangyou/qkv
- go build -i

## 集群、扩容、缩容
相关文档（主要是PD和TiKV扩容、缩容）：
- https://www.pingcap.com/docs-cn/

## 命令支持

### key
- DEL
- TTL
- PTTL
- EXPIRE
- PEXPIRE
- EXPIREAT
- PEXPIREAT

### string
- GET
- SET
- MGET
- MSET
- SETEX
- INCR
- INCRBY
- DECR
- DECRBY
- STRLEN

### set
- SADD
- SCARD
- SDIFF
- SDIFFSTORE
- SINTER
- SINTERSTORE
- SISMEMBER
- SMEMBERS
- SREM
- SUNION

### zset
- ZADD
- ZCARD
- ZCOUNT
- ZINCRBY
- ZLEXCOUNT
- ZRANGE
- ZRANGEBYLEX
- ZRANGEBYSCORE
- ZREM
- ZREMRANGEBYLEX
- ZREMRANGEBYSCORE
- ZREVRANGE
- ZREVRANGEBYLEX
- ZREVRANGEBYSCORE
- ZSCORE