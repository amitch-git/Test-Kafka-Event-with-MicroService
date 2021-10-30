package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "test"
	brokerAddress = "localhost:9092"
)

type Event struct {
	Name    string `json:"name"`
	Dept  string `json:"dept"`
	EmpID string `json:"id"`
	PunchTm string `json:"punch_time"`
}

func main() {
	// create a new context
	ctx := context.Background()
	// produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking
	go produce(ctx)
	consume(ctx)
}

func produce(ctx context.Context) {
	// initialize a counter
	i := 0

	l := log.New(os.Stdout, "kafka writer: ", 0)
	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		// assign the logger to the writer
		Logger: l,
	})

	
	for {
		//Make message
		t := time.Now()
		empId := 1111 + i;
		event := Event{
			Name: "Emp_" + strconv.Itoa(i),
			Dept: "OSS",
			EmpID: strconv.Itoa(empId),
			PunchTm: t.Format("02-01-2006 15:04:05"),
		}
		empData, err := json.Marshal(event)

		if err != nil {
			panic("could not convert to byte array " + err.Error())
		}

		err = w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: empData,
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for a second
		time.Sleep(5*time.Second)
	}
}

func consume(ctx context.Context) {
	// create a new logger that outputs to stdout
	// and has the `kafka reader` prefix
	l := log.New(os.Stdout, "kafka reader: ", 0)
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "my-group",
		// assign the logger to the reader
		Logger: l,
	})

	//Initialize http client
	client := &http.Client{}

	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))

		url := "http://localhost:8080/api/v1/punch"

		req, reqerr := http.NewRequest("POST", url, bytes.NewReader(msg.Value))
		if reqerr != nil {
			panic(reqerr)
		}
		req.Header.Set("Content-Type", "application/json")
		
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		
		resp.Body.Close()	
		fmt.Println("response Status:", resp.Status)
	}
}
