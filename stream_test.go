package qstream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/go-redis/redis/v7"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

var (
	//s.codec = StructCodec(SimpleData{})
	defaultCodec = &customizeCodec{}
	//defaultCodec = MsgpackCodec(SimpleData{})
)

type StreamTestSuite struct {
	suite.Suite
	redisClient *redis.Client
	codec       DataCodec
}

func (s *StreamTestSuite) SetupSuite() {
	viper.SetDefault("redis.url", "192.168.1.233:30790")
	viper.SetDefault("redis.password", "12345678")

	s.redisClient = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.url"),
		Password: viper.GetString("redis.password"),
		DB:       0,
	})

	s.codec = defaultCodec
}

func (s *StreamTestSuite) TearDownSuite() {
	s.redisClient.Close()
}

func (s *StreamTestSuite) TestStreamInterface() {
	var sub StreamSub
	var pub StreamPub

	stream := "qstream:test"
	pub = NewRedisStreamPub(s.redisClient, stream, 20, s.codec)

	if !s.Equal(stream, pub.GetKey()) {
		return
	}

	sub = NewRedisStreamSub(s.redisClient, s.codec, stream)

	if !s.Equal(stream, sub.GetKeys()[0]) {
		return
	}

	sub = NewRedisStreamGroupSub(s.redisClient, s.codec, "tester", "testerInterface", true, stream)
	if !s.Equal(stream, sub.GetKeys()[0]) {
		return
	}
}

func (s *StreamTestSuite) TestDataPubAndSub() {
	d := &SimpleData{
		ID:      1234,
		Message: "Hello",
	}

	stream := "qstream:test"

	pub := NewRedisStreamPub(s.redisClient, stream, 20, s.codec)
	streamID, err := pub.Send(d)

	if !s.NoError(err) {
		return
	}
	log.Infof("pub stream id:%s", streamID)

	sub := NewRedisStreamSub(s.redisClient, s.codec, stream)

	rlts, err := sub.Read(10, -1, "$")
	if !s.Error(redis.Nil) {
		return
	}

	rlts, err = sub.Read(10, -1, "0")

	if !s.NoError(err) {
		return
	}

	msgs, ok := rlts[stream]
	if !s.True(ok) {
		return
	}

	for _, m := range msgs {
		log.Infof("xread: %s : %#v", m.StreamID, m.Data)
		assert.Equal(s.T(), d, m.Data)
	}
}

func (s *StreamTestSuite) TestDataGroupRead() {
	d := &SimpleData{
		ID:      1234,
		Message: "Hello Group",
	}

	stream := "qstream:testgroup"

	pub := NewRedisStreamPub(s.redisClient, stream, 20, s.codec)
	streamID, err := pub.Send(d)

	if !s.NoError(err) {
		return
	}
	log.Infof("pub stream id:%s", streamID)

	sub := NewRedisStreamGroupSub(s.redisClient, s.codec, "testgroup", "testconsumer", false, stream)

	rlts, err := sub.Read(10, -1, ">")
	if !s.Error(redis.Nil) {
		return
	}

	rlts, err = sub.Read(10, -1, "0")

	if !s.NoError(err) {
		return
	}

	msgs, ok := rlts[stream]
	if !s.True(ok) {
		return
	}

	for _, m := range msgs {
		log.Infof("xgroupread: %s : %#v", m.StreamID, m.Data)
		assert.Equal(s.T(), d, m.Data)
	}
}

func TestStreams(t *testing.T) {
	suite.Run(t, new(StreamTestSuite))
}

func BenchmarkSteamRead(b *testing.B) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.233:30790",
		Password: "12345678",
		DB:       0,
	})

	defer redisClient.Close()

	stream := "qstream:test"
	sub := NewRedisStreamSub(redisClient, defaultCodec, stream)
	//sub := NewRedisStreamSub(redisClient, &customizeCodec{}, stream)

	for i := 0; i < b.N; i++ {
		rlt, err := sub.Read(100, -1, "0")
		_, _ = rlt, err
	}
}
