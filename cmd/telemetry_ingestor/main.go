package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	groundctrl "github.com/Boyul-Kim/groundCTRL/proto/telemetry"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type TelemetryIngestorServer struct {
	groundctrl.UnimplementedTelemetryIngestorServer
	KafkaWriter *kafka.Writer
}

func main() {
	fmt.Println("Telemetry Ingestor")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	ingestor := NewTelemetryIngestorServer()

	groundctrl.RegisterTelemetryIngestorServer(s, ingestor)
	log.Println("TELEMETRY INGESTOR SERVICE LISTENING ON :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func NewTelemetryIngestorServer() *TelemetryIngestorServer {
	topic := "telemetry"
	CreateKafkaTopic("telemetry")

	return &TelemetryIngestorServer{
		KafkaWriter: &kafka.Writer{
			Addr:     kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func CreateKafkaTopic(topic string) {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatalf("Error dialing kafka broker: %v", err)
	}

	defer conn.Close()

	partition := 1
	replication := 1

	err = conn.CreateTopics(
		kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     partition,
			ReplicationFactor: replication,
		},
	)
	if err != nil {
		log.Fatal("failed to create topic:", err)
	}
	log.Println("Topic created:", topic)
}

func (s *TelemetryIngestorServer) StreamData(stream groundctrl.TelemetryIngestor_StreamDataServer) error {
	count := 0
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Stream closed by client. Received %d chunks.", count)
			return nil
		}
		if err != nil {
			if err.Error() == "rpc error: code = Canceled desc = context canceled" {
				log.Printf("Client context canceled. Received %d chunks (connection closed).", count)
				return nil
			}
			log.Printf("Stream recv error: %v", err)
			return err
		}

		log.Printf("Received chunk: Satellite=%s Time=%d BusVoltage=%.2f SafeMode=%v",
			chunk.SatelliteId, chunk.Timestamp, chunk.BusVoltage, chunk.SafeMode)

		valueBytes, err := proto.Marshal(chunk)
		if err != nil {
			log.Printf("Failed to marshal chunk: %v", err)
			continue
		}

		err = s.KafkaWriter.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(chunk.SatelliteId),
				Value: valueBytes,
			},
		)

		if err != nil {
			log.Fatalf("Failed to publish topic to kafka broker: %v", err)
		}

		ack := &groundctrl.Ack{Status: "OK"}
		if err := stream.Send(ack); err != nil {
			log.Printf("Failed to send ack: %v", err)
			return err
		}
		count++
	}
}
