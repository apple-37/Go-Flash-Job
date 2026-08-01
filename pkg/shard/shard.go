package shard

import (
	"crypto/sha1"
	"encoding/hex"
	"strconv"

	"go-flash-job/pkg/consts"
)

// ShardKey 根据 jobID 计算分片 key
// 用 SHA1(jobID) % N 路由，保证同 ID 任务落到同一分片
func ShardKey(jobID string) string {
	h := sha1.Sum([]byte(jobID))
	hexStr := hex.EncodeToString(h[:8]) // 取前 8 字节够用了
	// 把 hex 前 8 位转成 uint64
	var n uint64
	for i := 0; i < 16; i += 2 {
		c := hexStr[i]
		var v byte
		if c >= '0' && c <= '9' {
			v = c - '0'
		} else {
			v = c - 'a' + 10
		}
		n = n*16 + uint64(v)
	}
	idx := n % uint64(consts.JobShardCount)
	return consts.JobShardPrefix + strconv.FormatUint(idx, 10)
}

// AllShardKeys 返回所有分片 key（按 0,1,2,...,N-1 顺序）
// 用于 fetcherLoop 遍历所有分片拉取任务
func AllShardKeys() []string {
	keys := make([]string, consts.JobShardCount)
	for i := 0; i < consts.JobShardCount; i++ {
		keys[i] = consts.JobShardPrefix + strconv.Itoa(i)
	}
	return keys
}
