package kafkastreams_test

import (
	"context"
	"math/rand"
	"testing"

	kafkastreams "github.com/pieper126/KafkaStreams"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type Reader[T any] interface {
	ReadMessage() T
}

type MockReader struct {
}

func (m MockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	return kafka.Message{}, nil
}

func TestIsAbleToRecieveMessage(t *testing.T) {
	reader := MockReader{}
	bufferSize := 10
	cancelChannel := make(chan bool, 2)

	expectedFirstMessage := kafka.Message{}

	received := make(chan kafka.Message, 2)

	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		cancelChannel <- true
		received <- message
		break
	}

	firstMessage := <-received

	assert.Equal(t, expectedFirstMessage, firstMessage)
}

func TestAbleToReceiveMultipleMessages(t *testing.T) {
	reader := MockReader{}
	bufferSize := 10
	cancelChannel := make(chan bool, 2)

	numberOfMessage := rand.Int() % 100
	received := make(chan kafka.Message, numberOfMessage)

	expectedMessage := kafka.Message{}

	i := 0
	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		cancelChannel <- true
		received <- message
		i += 1
		if numberOfMessage == i {
			break
		}
	}

	for numberReceived := 0; numberReceived < numberOfMessage; numberReceived++ {
		message := <-received
		assert.Equal(t, expectedMessage, message)
	}
}
