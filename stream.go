package qstream

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrAckNotRequired  = errors.New("ErrAckNotRequired")
	ErrInvalidStreamID = errors.New("ErrInvalidStreamID")
	ErrMessageTrimmed  = errors.New("data trimmed")
)

type StreamPub interface {
	Send(ctx context.Context, data interface{}) (string, error)
	GetKey() string
}

type StreamSub interface {
	Read(ctx context.Context, count int64, block time.Duration, ids ...string) (map[string][]StreamSubResult, error)
	GetKeys() []string
	GetKeyIndex(key string) int
	Ack(ctx context.Context, streamKeyOrIndex interface{}, msgIDs ...string) error
}

type StreamSubResult struct {
	StreamID string
	Data     interface{}
	Err      error // Data codec err
}

type RedisStreamPub struct {
	redisClient *redis.Client
	StreamKey   string
	MaxLen      int64
	codec       DataCodec
}

func (s *RedisStreamPub) Send(ctx context.Context, data interface{}) (string, error) {
	vals, err := s.codec.Encode(data)
	if err != nil {
		return "", err
	}

	rlt, err := s.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: s.StreamKey,
		MaxLen: s.MaxLen,
		Approx: true,
		Values: vals,
	}).Result()

	if err != nil {
		return "", err
	}

	return rlt, nil
}

func (s *RedisStreamPub) GetKey() string {
	return s.StreamKey
}

func NewRedisStreamPub(redisClient *redis.Client, key string, maxLen int64, codec DataCodec) *RedisStreamPub {
	return &RedisStreamPub{
		redisClient: redisClient,
		StreamKey:   key,
		MaxLen:      maxLen,
		codec:       codec,
	}
}

func XMessage2Data(xmsg []redis.XMessage, codec DataCodec) ([]StreamSubResult, error) {
	msgs := make([]StreamSubResult, len(xmsg))

	for i, msg := range xmsg {
		if len(msg.Values) == 0 { // may the msg is trimmed and become nil
			msgs[i] = StreamSubResult{
				StreamID: msg.ID,
				Err:      ErrMessageTrimmed,
			}
			continue
		}

		d, err := codec.Decode(msg.Values)

		if err != nil {
			msgs[i] = StreamSubResult{
				StreamID: msg.ID,
				Err:      err,
			}
			continue
		}

		msgs[i] = StreamSubResult{
			StreamID: msg.ID,
			Data:     d,
		}
	}
	return msgs, nil
}

func XStream2Data(xstream []redis.XStream, codec DataCodec) (map[string][]StreamSubResult, error) {
	rlt := make(map[string][]StreamSubResult)

	for _, stream := range xstream {
		if len(stream.Messages) == 0 {
			rlt[stream.Stream] = nil
			continue
		}
		msgs, err := XMessage2Data(stream.Messages, codec)

		if err != nil {
			return nil, err
		}

		rlt[stream.Stream] = msgs
	}
	return rlt, nil
}

func makeKeyIndex(keys []string) map[string]int {
	idxMap := make(map[string]int)
	for i, key := range keys {
		idxMap[key] = i
	}
	return idxMap
}

type baseRedisStreamSub struct {
	redisClient *redis.Client
	keyIdx      map[string]int
	StreamKeys  []string
	codec       DataCodec
}

func (s *baseRedisStreamSub) GetKeys() []string {
	return s.StreamKeys
}

func (s *baseRedisStreamSub) GetKeyIndex(key string) int {
	idx, ok := s.keyIdx[key]
	if !ok {
		return -1
	}
	return idx
}

func newBaseRedisStreamSub(redisClient *redis.Client, codec DataCodec, streamKeys ...string) *baseRedisStreamSub {
	return &baseRedisStreamSub{
		redisClient: redisClient,
		keyIdx:      makeKeyIndex(streamKeys),
		StreamKeys:  streamKeys,
		codec:       codec,
	}
}

type RedisStreamSub struct {
	*baseRedisStreamSub
}

func (s *RedisStreamSub) Read(ctx context.Context, count int64, block time.Duration, ids ...string) (map[string][]StreamSubResult, error) {
	streams := s.StreamKeys
	if ids == nil {
		for range s.StreamKeys {
			streams = append(streams, "$")
		}
	} else {
		streams = append(s.StreamKeys, ids...)
	}

	rlt, err := s.redisClient.XRead(ctx, &redis.XReadArgs{
		Streams: streams,
		Count:   count,
		Block:   block,
	}).Result()

	// conn := s.redisClient.Conn()
	// connectionID := conn.ClientID(ctx).Val()

	// stop := context.AfterFunc(ctx, func() {
	// 	// uses one of the pooled connections of the redis client to unblock the blocking connection
	// 	s.redisClient.ClientUnblock(context.Background(), connectionID)
	// })
	// defer stop()

	// rlt, err := conn.XRead(ctx, &redis.XReadArgs{
	// 	Streams: streams,
	// 	Count:   count,
	// 	Block:   block,
	// }).Result()

	if err != nil {
		return nil, err
	}

	return XStream2Data(rlt, s.codec)
}

func (s *RedisStreamSub) Ack(ctx context.Context, streamKeyOrIndex interface{}, msgIDs ...string) error {
	return ErrAckNotRequired
}

func NewRedisStreamSub(redisClient *redis.Client, codec DataCodec, keys ...string) *RedisStreamSub {
	return &RedisStreamSub{
		baseRedisStreamSub: newBaseRedisStreamSub(redisClient, codec, keys...),
	}
}

type RedisStreamGroupSub struct {
	*baseRedisStreamSub
	Group        string
	Consumer     string
	NoAck        bool
	GroupStartID string
	checkOnce    sync.Once
	checkErr     error
}

func (s *RedisStreamGroupSub) checkStreamAndGroup(ctx context.Context) {
	var err error
	// do not check redis
	// if err = s.redisClient.Ping().Err(); err != nil {
	// 	s.checkErr = err
	// 	return
	// }

	for _, stream := range s.StreamKeys {
		err = s.redisClient.XGroupCreate(ctx, stream, s.Group, s.GroupStartID).Err()

		if err == nil {
			continue //create group ok, just continue
		}

		if strings.HasPrefix(err.Error(), "BUSYGROUP") {
			continue // group already exist, just continue
		}

		// may the streamid not exist, create group with an empty group
		err = s.redisClient.XGroupCreateMkStream(ctx, stream, s.Group, s.GroupStartID).Err()

		if err != nil {
			s.checkErr = err // still has error, should be redis problem, in this case, the GroupSub cannot recover
		}
	}
}

// Read will create the group if stream exist, and will create an empty stream if stream no exist
// note if pass id ">" and no new data, it will return redis.Nil,
// if pass id "0-0", it will return a stream name map with zero length data
func (s *RedisStreamGroupSub) Read(ctx context.Context, count int64, block time.Duration, ids ...string) (map[string][]StreamSubResult, error) {
	s.checkOnce.Do(func() { s.checkStreamAndGroup(ctx) })

	if s.checkErr != nil {
		return nil, s.checkErr
	}

	streams := s.StreamKeys
	if ids == nil {
		for range s.StreamKeys {
			streams = append(streams, ">")
		}
	} else {
		streams = append(s.StreamKeys, ids...)
	}

	// conn := s.redisClient.Conn()
	// connectionID := conn.ClientID(ctx).Val()

	// stop := context.AfterFunc(ctx, func() {
	// 	// uses one of the pooled connections of the redis client to unblock the blocking connection
	// 	s.redisClient.ClientUnblock(context.Background(), connectionID)
	// })
	// defer stop()

	rlt, err := s.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.Group,
		Consumer: s.Consumer,
		Streams:  streams,
		Count:    count,
		Block:    block,
		NoAck:    s.NoAck,
	}).Result()

	if err != nil {
		return nil, err
	}

	return XStream2Data(rlt, s.codec)
}

func (s *RedisStreamGroupSub) Ack(ctx context.Context, streamKeyOrIndex interface{}, msgIDs ...string) error {

	switch k := streamKeyOrIndex.(type) {
	case int:
		if k < len(s.StreamKeys) {
			return s.redisClient.XAck(ctx, s.StreamKeys[k], s.Group, msgIDs...).Err()
		}
	case string:
		for _, key := range s.StreamKeys { // find a valid stream key
			if key == k {
				return s.redisClient.XAck(ctx, k, s.Group, msgIDs...).Err()
			}
		}
		return s.redisClient.XAck(ctx, k, s.Group, msgIDs...).Err()
	}

	return ErrInvalidStreamID
}

func NewRedisStreamGroupSub(redisClient *redis.Client, codec DataCodec, group string, groupStartID string, consumer string, noack bool, keys ...string) *RedisStreamGroupSub {
	if !redisClient.Options().ContextTimeoutEnabled {
		panic("redis client must enable context timeout")
	}

	return &RedisStreamGroupSub{
		baseRedisStreamSub: newBaseRedisStreamSub(redisClient, codec, keys...),
		Group:              group,
		GroupStartID:       groupStartID,
		Consumer:           consumer,
		NoAck:              noack,
	}
}
