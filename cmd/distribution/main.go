package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	groundctrl "github.com/Boyul-Kim/groundCTRL/proto/telemetry"
)

type DistributionServer struct {
	KafkaReader *kafka.Reader
}

func main() {
	distribution := NewDistributionServer()

	distribution.ReadFromKafka()
}

func NewDistributionServer() *DistributionServer {
	groupId := "groundctrl"
	topic := "telemetry"

	return &DistributionServer{
		KafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			GroupID:  groupId,
			Topic:    topic,
			MaxBytes: 10e6, // 10MB
		}),
	}
}

func (s *DistributionServer) ReadFromKafka() {
	fmt.Println("READING...")
	for {
		m, err := s.KafkaReader.ReadMessage(context.Background())
		if err != nil {
			break
		}

		var chunk groundctrl.DataChunk
		if err := proto.Unmarshal(m.Value, &chunk); err != nil {
			log.Printf("Failed to unmarshal proto: %v", err)
			continue
		}

		log.Printf("Consumed topic: Satellite=%s Time=%d BusVoltage=%.2f SafeMode=%v",
			m.Key, chunk.Timestamp, chunk.BusVoltage, chunk.SafeMode)
	}

	if err := s.KafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
