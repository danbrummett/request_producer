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
    "time"
    "context"
)

type request struct {
    requestor string `json:"requestor"`
    operator string `json:"operator"`
    param1 int `json:"param1"`
    param2 int `json:"param2"`
}

func main () {
    names := strings.Split(os.Getenv("PRODUCER_NAMES"), ",")
    raw_topic, exists := os.LookupEnv("PRODUCER_RAW_TOPIC")
    kafka_brokers := strings.Split(os.Getenv("BROKER_LIST"), ",")
    request_count, err := strconv.Atoi(os.Getenv("REQUEST_COUNT"))
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
    })
    defer writer.Close()

    if err != nil {
        fmt.Printf("Failed to create producer: %s\n", err)
        os.Exit(1)
    }

    requests := make(chan request, 500)
    go randomRequest(request_count, names, 1000, requests)
    go send(writer, raw_topic, requests)
}

//func send(p *kafka.Producer, raw_topic string, c chan request) {
func send(w *kafka.Writer, raw_topic string, c chan request) {
    //delivery_chan := make(chan kafka.Event, 10000)
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
        time.Sleep(1 * time.Second)
    }
}

func randomRequest(request_count int, names []string, param_max int, c chan request) {
    for i := 0; i < request_count; i++ {
        operators := []string{"add", "subtract", "multiply", "divide"}
        name_idx := rand.Intn(len(names) - 1)
        operator_idx := rand.Intn(len(operators) - 1)
        r := request{names[name_idx], operators[operator_idx], rand.Intn(param_max), rand.Intn(param_max)}
        c <- r
    }
}
