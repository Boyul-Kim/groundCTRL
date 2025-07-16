package main

import (
	"fmt"
	"io"
	"log"
	"net"

	groundctrl "github.com/Boyul-Kim/groundCTRL/proto/telemetry"
	"google.golang.org/grpc"
)

type TelemetryIngestorServer struct {
	groundctrl.UnimplementedTelemetryIngestorServer
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
	return &TelemetryIngestorServer{}
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

		ack := &groundctrl.Ack{Status: "OK"}
		if err := stream.Send(ack); err != nil {
			log.Printf("Failed to send ack: %v", err)
			return err
		}
		count++
	}
}
