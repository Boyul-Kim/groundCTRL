package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	groundctrl "github.com/Boyul-Kim/groundCTRL/proto/telemetry"
)

type DistributionServer struct {
	KafkaReader *kafka.Reader
	PgConn      *pgx.Conn
	RedisConn   *redis.Client
}

func main() {
	distribution := NewDistributionServer()
	distribution.ReadFromKafka()
}

func NewDistributionServer() *DistributionServer {
	groupId := "groundctrl"
	topic := "telemetry"

	pgConn, err := pgx.Connect(context.Background(),
		"postgres://postgres:mysecretpassword@localhost:5432/timeseries")
	if err != nil {
		log.Fatalf("Unable to connect to TimescaleDB: %v", err)
	}

	if err := initTelemetryTable(pgConn); err != nil {
		log.Fatalf("Unable to initialize telemetry table: %v", err)
	}

	redisConn := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return &DistributionServer{
		KafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			GroupID:  groupId,
			Topic:    topic,
			MaxBytes: 10e6, // 10MB
		}),
		PgConn:    pgConn,
		RedisConn: redisConn,
	}
}

func (s *DistributionServer) ReadFromKafka() {
	fmt.Println("READING...")
	for {
		ctx := context.Background()
		m, err := s.KafkaReader.ReadMessage(ctx)
		if err != nil {
			break
		}

		var chunk groundctrl.DataChunk
		if err := proto.Unmarshal(m.Value, &chunk); err != nil {
			log.Printf("Failed to unmarshal proto: %v", err)
			continue
		}

		_, err = s.PgConn.Exec(ctx,
			`INSERT INTO telemetry (
                satellite_id, timestamp, bus_voltage, battery_temp, cpu_temp,
                solar_panel_current, attitude_mode, gyro_x, gyro_y, gyro_z,
                sun_sensor_angle, comm_link, last_command_ack, error_flags, safe_mode
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
            )`,
			chunk.SatelliteId,
			chunk.Timestamp,
			chunk.BusVoltage,
			chunk.BatteryTemp,
			chunk.CpuTemp,
			chunk.SolarPanelCurrent,
			chunk.AttitudeMode,
			chunk.GyroX,
			chunk.GyroY,
			chunk.GyroZ,
			chunk.SunSensorAngle,
			chunk.CommLink,
			chunk.LastCommandAck,
			chunk.ErrorFlags,
			chunk.SafeMode,
		)

		if err != nil {
			log.Printf("Failed to insert into TimescaleDB: %v", err)
		}

		err = s.RedisConn.Set(ctx, chunk.SatelliteId, m.Value, 0).Err()

		//will need to use when getting the value from redis
		// data, err := s.RedisConn.Get(ctx, chunk.SatelliteId).Bytes()
		// if err != nil {

		// }
		// var cachedChunk groundctrl.DataChunk
		// if err := proto.Unmarshal(data, &cachedChunk); err != nil {

		// }

		log.Printf("Consumed topic: Satellite=%s Time=%d BusVoltage=%.2f SafeMode=%v",
			m.Key, chunk.Timestamp, chunk.BusVoltage, chunk.SafeMode)
	}

	if err := s.KafkaReader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

	if err := s.PgConn.Close(context.Background()); err != nil {
		log.Fatal("failed to close DB connection:", err)
	}
}

func initTelemetryTable(conn *pgx.Conn) error {
	ctx := context.Background()

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS telemetry (
		satellite_id    TEXT,
		timestamp       BIGINT,
		bus_voltage     REAL,
		battery_temp    REAL,
		cpu_temp        REAL,
		solar_panel_current REAL,
		attitude_mode   TEXT,
		gyro_x          REAL,
		gyro_y          REAL,
		gyro_z          REAL,
		sun_sensor_angle REAL,
		comm_link       TEXT,
		last_command_ack TEXT,
		error_flags     INT,
		safe_mode       BOOL
	);`
	_, err := conn.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create telemetry table: %w", err)
	}

	_, err = conn.Exec(ctx, `SELECT create_hypertable('telemetry', 'timestamp', if_not_exists => TRUE);`)
	if err != nil {
		return fmt.Errorf("failed to create hypertable: %w", err)
	}
	return nil
}
