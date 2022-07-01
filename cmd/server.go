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
package cmd

import (
	"fmt"
	"log"
	"net"
	"os"

	"github.com/Colocasian/qmonster/internal/queue"
	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
	"github.com/spf13/cobra"
)

var args struct {
	addr   string
	credit int
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Runs the message broker server",
	Run: func(cmd *cobra.Command, _ []string) {
		b := &broker{
			ex:        queue.NewExchange(),
			container: electron.NewContainer(fmt.Sprintf("broker[%v]", os.Getpid())),
			sent:      make(chan sentMessage),
			acks:      make(chan electron.Outcome),
		}

		if err := b.run(args.addr); err != nil {
			log.Fatalln(err)
		}
	},
}

type broker struct {
	ex        *queue.Exchange
	container electron.Container
	sent      chan sentMessage
	acks      chan electron.Outcome
}

type sentMessage struct {
	m   *amqp.Message
	qid string
}

func (b *broker) run(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Printf("Listening on %v\n", listener.Addr())

	go b.acknowledgements()

	for {
		c, err := b.container.Accept(listener)
		if err != nil {
			continue
		}
		cc := &connection{b, c}
		go cc.run()
	}
}

type connection struct {
	broker     *broker
	connection electron.Connection
}

func (cc *connection) run() {
	for in := range cc.connection.Incoming() {
		switch in := in.(type) {
		case *electron.IncomingSender:
			s := in.Accept().(electron.Sender)
			go cc.sender(s)
		case *electron.IncomingReceiver:
			in.SetPrefetch(true)
			in.SetCapacity(args.credit)
			r := in.Accept().(electron.Receiver)
			go cc.receiver(r)
		default:
			in.Accept()
		}
	}
}

func (cc *connection) sender(sender electron.Sender) {
	q, ok := cc.broker.ex.Load(sender.Source())
	if !ok {
		return
	}
	for {
		if sender.Error() != nil {
			return
		}
		select {
		case m := <-q.Chan():
			sm := sentMessage{m, sender.Source()}
			cc.broker.sent <- sm
			sender.SendAsync(*m, cc.broker.acks, sm)

		case <-sender.Done():
			break
		}
	}
}

func (cc *connection) receiver(receiver electron.Receiver) {
	q, ok := cc.broker.ex.Load(receiver.Target())
	if !ok {
		return
	}
	for {
		if rm, err := receiver.Receive(); err == nil {
			q.TryEnqueue(&rm.Message)
			rm.Accept()
		} else {
			break
		}
	}
}

func (b *broker) acknowledgements() {
	sentMap := make(map[sentMessage]bool)
	for {
		select {
		case sm, ok := <-b.sent:
			if ok {
				sentMap[sm] = true
			} else {
				return
			}
		case outcome := <-b.acks:
			sm := outcome.Value.(sentMessage)
			delete(sentMap, sm)
			if outcome.Status != electron.Accepted {
				q, _ := b.ex.Load(sm.qid)
				q.TryEnqueue(sm.m)
			}
		}
	}
}

func init() {
	serverCmd.Flags().StringVarP(&args.addr, "addr", "a", ":35254", "TCP address to attach AMQP server")
	serverCmd.Flags().IntVarP(&args.credit, "credit", "c", 100, "Reciever credit window")

	rootCmd.AddCommand(serverCmd)
}
