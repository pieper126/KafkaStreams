package kafkastreams

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
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

	workerGroup, groupCtx := errgroup.WithContext(context.Background())
	ctx, cancelGroup := context.WithCancel(groupCtx)

	workerGroup.SetLimit(5)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				workerGroup.Go(func() error {
					msg, err := reader.ReadMessage(ctx)
					if err != nil {
						if strings.Contains(err.Error(), "context canceled") {
							return nil
						}
						log.Println("Error reading message:", err)
						time.Sleep(time.Second)

						return nil
					}

					select {
					case <-ctx.Done():
						return nil
					case res <- msg:
						return nil
					}
				})
			}
		}
	}()

	go func() {
		<-cancel
		cancelGroup()

		err := workerGroup.Wait()
		if err != nil {
			fmt.Println(err)
		}

		close(res)
	}()

	return res
}
