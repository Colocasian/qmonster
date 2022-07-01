/*
Copyright 2022 Rishvic Pushpakaran

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package queue

import (
	"errors"

	"github.com/apache/qpid-proton/go/pkg/amqp"
)

type Queue interface {
	TryEnqueue(m *amqp.Message) error
	Dequeue() *amqp.Message
	Chan() chan *amqp.Message
}

// Basic implementation of Queue using Go channels
type ChanQueue chan *amqp.Message

func NewChannelQueue(mem int) *ChanQueue {
	ch := ChanQueue(make(chan *amqp.Message, mem))
	return &ch
}

func (q *ChanQueue) TryEnqueue(m *amqp.Message) error {
	select {
	case *q <- m:
	default:
		return errors.New("buffer overflow")
	}
	return nil
}

func (q *ChanQueue) Dequeue() *amqp.Message {
	return <-*q
}

func (q *ChanQueue) Chan() chan *amqp.Message {
	return chan *amqp.Message(*q)
}
