package kafkastreams_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	kafkastreams "github.com/pieper126/KafkaStreams"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type Reader[T any] interface {
	ReadMessage() T
}

func TestIsAbleToRecieveMessage(t *testing.T) {
	reader := NewErrorReader(0)
	bufferSize := 10
	cancelChannel := make(chan bool, 2)

	expectedFirstMessage := kafka.Message{}

	received := make(chan kafka.Message, 2)

	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		received <- message
		cancelChannel <- true
		break
	}

	firstMessage := <-received

	assert.Equal(t, expectedFirstMessage, firstMessage)
}

func TestAbleToReceiveMultipleMessages(t *testing.T) {
	reader := NewErrorReader(0)
	bufferSize := 10
	cancelChannel := make(chan bool, 2)

	numberOfMessage := rand.Int() % 100
	received := make(chan kafka.Message, numberOfMessage)

	expectedMessage := kafka.Message{}

	i := 0
	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		received <- message
		i += 1
		if numberOfMessage == i {
			cancelChannel <- true
			break
		}
	}

	for numberReceived := 0; numberReceived < numberOfMessage; numberReceived++ {
		message := <-received
		assert.Equal(t, expectedMessage, message)
	}
}

func TestAbleToCancelWithSlowReader(t *testing.T) {
	reader := NewSlowReader(10 * time.Millisecond)

	bufferSize := 10
	cancelChannel := make(chan bool, 2)

	numberOfMessage := rand.Int() % 100
	received := make(chan kafka.Message, numberOfMessage)

	expectedMessage := kafka.Message{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	i := 0
	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		select {
		case <-ctx.Done():
			break
		case <-time.After(10 * time.Second):
			assert.FailNow(t, "should be done before timer")
		case received <- message:
			i += 1
			if numberOfMessage == i {
				cancelChannel <- true
				cancel()
				break
			}
		}
	}

	for numberReceived := 0; numberReceived < numberOfMessage; numberReceived++ {
		message := <-received
		assert.Equal(t, expectedMessage, message)
	}
}

func TestAbleToCancel(t *testing.T) {
	reader := NewErrorReader(0)

	bufferSize := 100
	cancelChannel := make(chan bool, 1)

	received := make(chan kafka.Message, 2)

	ctx, localCancel := context.WithCancel(context.Background())
	defer localCancel()

	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		select {
		case <-ctx.Done():
			break
		case <-time.After(100 * time.Millisecond):
			cancelChannel <- true
			localCancel()
			break
		case received <- message:
		}
	}

	assert.True(t, true)
}

func TestAbleTocancelWithDelay(t *testing.T) {
	reader := NewSlowReader(100 * time.Millisecond)

	bufferSize := 100
	cancelChannel := make(chan bool, 1)

	received := make(chan kafka.Message, 2)

	ctx, localCancel := context.WithCancel(context.Background())
	defer localCancel()

	for message := range kafkastreams.Streams[kafka.Message](reader, bufferSize, cancelChannel) {
		select {
		case <-ctx.Done():
			break
		case <-time.After(100 * time.Millisecond):
			cancelChannel <- true
			localCancel()
			break
		case received <- message:
		}
	}

	assert.True(t, true)
}

type SlowReader struct {
	delay time.Duration
}

func NewSlowReader(delay time.Duration) *SlowReader {
	return &SlowReader{
		delay: delay,
	}
}

func (m *SlowReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, fmt.Errorf("context canceled")
	case <-time.After(m.delay):
		return kafka.Message{}, nil
	}
}

type ErrorReader struct {
	iterations               int
	numberOfExpectedFailures int
}

func NewErrorReader(numberOfFailures int) *ErrorReader {
	return &ErrorReader{
		iterations:               0,
		numberOfExpectedFailures: numberOfFailures,
	}
}

func (m *ErrorReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.iterations < m.numberOfExpectedFailures {
		m.iterations += 1
		return kafka.Message{}, fmt.Errorf("expected failure")
	} else {
		return kafka.Message{}, nil
	}
}
