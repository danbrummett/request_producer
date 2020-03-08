package main

import (
    kafka "github.com/segmentio/kafka-go"
    "os"
    "math/rand"
    "encoding/json"
    "fmt"
    "strconv"
    "strings"
    "github.com/google/uuid"
    "context"
)

type request struct {
    Requestor string
    Operator string
    Param1 int
    Param2 int
}

func main () {
    names := strings.Split(os.Getenv("PRODUCER_NAMES"), ",")
    raw_topic, exists := os.LookupEnv("PRODUCER_RAW_TOPIC")
    kafka_brokers := strings.Split(os.Getenv("BROKER_LIST"), ",")
    request_count, err := strconv.Atoi(os.Getenv("REQUEST_COUNT"))

    if err != nil {
      request_count = 1000
    }
    if ! exists {
        fmt.Printf("REQUIRED env variables : \n\t%s\n\t%s\n\t%s\n\t%s\n",
	    "PRODUCER_NAMES", "PRODUCER_RAW_TOPIC", "BROKER_LIST", "REQUEST_COUNT")
	os.Exit(0)
    }

    //producer_conf := kafka.ConfigMap{
        //"bootstrap.servers":  kafka_brokers,
    //}

    //p, err := kafka.NewProducer(&producer_conf)
    writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafka_brokers,
		Topic:    raw_topic,
		Balancer: &kafka.LeastBytes{},
		//Async : true,
    })
    defer writer.Close()

    //if err != nil {
        //fmt.Printf("Failed to create producer: %s\n", err)
        //os.Exit(1)
    //}

    fmt.Println("starting to create requests")
    operators := []string{"add", "subtract", "multiply", "divide"}
    param_max := 1000
    batch_size := 100
    var msg_batch []kafka.Message
    for i := 0; i <= request_count; i++ {
        name_idx := 0
        if len(names) > 1 {
          name_idx = rand.Intn(len(names) - 1)
        }
        operator_idx := rand.Intn(len(operators) - 1)
        r := &request{
	    Requestor: names[name_idx],
	    Operator: operators[operator_idx],
	    Param1: rand.Intn(param_max),
	    Param2: rand.Intn(param_max),
	}

        json_request, err := json.Marshal(r)
        fmt.Printf("starting send %s\n", string(json_request))
	if err != nil {
	    fmt.Printf("request marshal to json error %s\n", err)
	}
        msg := kafka.Message{
            Key:   []byte(fmt.Sprintf("%d",uuid.New())),
            Value: json_request,
        }
	if len(msg_batch) > batch_size {
            err = writer.WriteMessages(context.Background(), msg_batch...)
	    if err != nil {
                fmt.Println(err)
            }
            msg_batch = []kafka.Message{msg}
        } else {
	    msg_batch = append(msg_batch, msg)
	}
    }
    err = writer.WriteMessages(context.Background(), msg_batch...)
    if err != nil {
        fmt.Println(err)
    }

    //requests := make(chan request, 500)
    //randomRequest(500, names, 1000, requests)
    //send(writer, raw_topic, requests)
}

//func send(p *kafka.Producer, raw_topic string, c chan request) {
func send(w *kafka.Writer, raw_topic string, c chan request) {
    //delivery_chan := make(chan kafka.Event, 10000)
    fmt.Println("starting send")
    for r := range c {
        json_request, err := json.Marshal(r)
        //err = p.Produce(&kafka.Message{
            //TopicPartition: kafka.TopicPartition{Topic: &raw_topic, Partition: kafka.PartitionAny},
            //Value: json_request})
        msg := kafka.Message{
            Key:   []byte(fmt.Sprintf("%d",uuid.New())),
            Value: json_request,
        }
        err = w.WriteMessages(context.Background(), msg)
        if err != nil {
            fmt.Println(err)
        }
	fmt.Println("sent message")
    }
    w.Close()
}

func randomRequest(request_count int, names []string, param_max int, c chan request) {
    fmt.Println("starting to create requests")
    operators := []string{"add", "subtract", "multiply", "divide"}
    for i := 0; i <= request_count; i++ {
        name_idx := 0
	if len(names) > 1 {
          name_idx = rand.Intn(len(names) - 1)
	}
        operator_idx := rand.Intn(len(operators) - 1)
        r := request{names[name_idx], operators[operator_idx], rand.Intn(param_max), rand.Intn(param_max)}
        c <- r
	fmt.Printf("%d request created\n", i)
    }
}
