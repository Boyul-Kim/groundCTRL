package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	groundctrl "github.com/Boyul-Kim/groundCTRL/proto/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var satellites = []string{
	"SAT-001",
	"SAT-002",
	"SAT-003",
	"SAT-004",
	"SAT-005",
}

func main() {
	fmt.Println("Ground Station")

	//uncomment if test_telemetry.bin has not been generated yet
	//generateTestTelemetryData()

	readTeletryData()
}

func readTeletryData() {
	f, err := os.Open("test_telemetry.bin")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := groundctrl.NewTelemetryIngestorClient(conn)
	ctx := context.Background()

	stream, err := client.StreamData(ctx)
	if err != nil {
		panic(err)
	}

	// track how many we send
	sendCount := 0

	for {
		var length uint32
		if err := binary.Read(f, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("failed to read length: %v", err)
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(f, buf); err != nil {
			log.Fatalf("failed to read packet: %v", err)
		}
		var chunk groundctrl.DataChunk
		if err := proto.Unmarshal(buf, &chunk); err != nil {
			log.Fatalf("failed to unmarshal: %v", err)
		}
		if err := stream.Send(&chunk); err != nil {
			log.Fatalf("failed to send: %v", err)
		}
		sendCount++
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("Finished sending %d chunks, closing send side of stream.\n", sendCount)
	err = stream.CloseSend()
	if err != nil {
		log.Printf("stream close error: %v", err)
	}

	ackCount := 0
	for {
		ack, err := stream.Recv()
		if err == io.EOF {
			break // all acks received
		}
		if err != nil {
			log.Printf("Ack recv error: %v", err)
			break
		}
		ackCount++
		log.Printf("Received ack: %v", ack.Status)
	}
	fmt.Printf("Done and closing. Received %d acks\n", ackCount)
}

func generateTestTelemetryData() {
	fmt.Println("Generating test telemetry data...")
	source := rand.NewSource(time.Now().UnixNano())
	rand.New(source)

	f, err := os.Create("test_telemetry.bin")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	for _, sat := range satellites {
		start := time.Now().Unix()

		for i := 0; i < 200; i++ {
			dataChunk := &groundctrl.DataChunk{
				SatelliteId:       sat,
				Timestamp:         start + int64(i*5),
				BusVoltage:        float32(28.0 + rand.Float64()*2 - 1),
				BatteryTemp:       float32(25.0 + rand.Float64()*6 - 3),
				CpuTemp:           float32(50.0 + rand.Float64()*10 - 5),
				SolarPanelCurrent: float32(2.0 + rand.Float64()*0.6 - 0.3),
				AttitudeMode:      []string{"Nadir-Pointing", "Sun-Pointing", "Safe"}[rand.Intn(3)],
				GyroX:             float32(rand.Float64()*0.004 - 0.002),
				GyroY:             float32(rand.Float64()*0.004 - 0.002),
				GyroZ:             float32(rand.Float64()*0.004 - 0.002),
				SunSensorAngle:    float32(rand.Float64() * 180),
				CommLink:          []string{"ACTIVE", "IDLE", "ERROR"}[rand.Intn(3)],
				LastCommandAck:    []string{"SUCCESS", "FAIL", "PENDING"}[rand.Intn(3)],
				ErrorFlags:        int32(rand.Intn(4)),
				SafeMode:          rand.Intn(10) == 0,
			}
			bin, err := proto.Marshal(dataChunk)
			if err != nil {
				panic(err)
			}

			// 4-byte length prefix
			if err := binary.Write(f, binary.BigEndian, uint32(len(bin))); err != nil {
				panic(err)
			}

			if _, err := f.Write(bin); err != nil {
				panic(err)
			}
		}
	}
	fmt.Println("Test telemetry data successfully generated")
}
