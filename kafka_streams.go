package kafkastreams

import (
	"context"
	"log"
	"strings"
	"time"
)

type Message[T any] interface {
	IntoKafkaMessage() T
}

type Reader[T any] interface {
	ReadMessage(ctx context.Context) (T, error)
}

type Stream[T any] chan T

func Streams[T any](reader Reader[T], bufferSize int, cancel chan bool) chan T {
	res := make(chan T, bufferSize)

	go func() {
		ctx, localCancel := context.WithCancel(context.Background())
		for {
			select {
			case <-cancel:
				localCancel()
				break
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if strings.Contains(err.Error(), "context canceled") {
						break
					}
					log.Println("Error reading message:", err)
					time.Sleep(time.Second)
					continue
				}
				res <- msg
			}
		}
	}()

	return res
}
