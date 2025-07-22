package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

const (
	paymentTopic = "payment-events"
	userTopic    = "user-events"
	movieTopic   = "movie-events"
)

type successBody struct {
	Status string `json:"status"`
}

func main() {
	e := initKafka()
	if e != nil {
		log.Fatalf("can't init kafka: %s", e)
	}

	// Set up HTTP routes
	http.HandleFunc("/api/events/payment", handlePayment)
	http.HandleFunc("/api/events/user", handleUser)
	http.HandleFunc("/api/events/movie", handleMovie)
	http.HandleFunc("/api/events/health", handleHealth)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082" // Note: Using a different port than the monolith
	}
	log.Printf("Starting events microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

var kafkaSendChannel = make(chan kafka.Message, 10)

func initKafka() error {
	broker := os.Getenv("KAFKA_BROKERS")

	userReader := kafka.NewReader(kafka.ReaderConfig{
		Partition: 0,
		Topic:     userTopic,
		Brokers:   strings.Split(broker, ","),
	})
	userWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(broker, ","),
		Topic:   userTopic,
	})
	movieReader := kafka.NewReader(kafka.ReaderConfig{
		Partition: 0,
		Topic:     movieTopic,
		Brokers:   strings.Split(broker, ","),
	})
	movieWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(broker, ","),
		Topic:   movieTopic,
	})
	paymentReader := kafka.NewReader(kafka.ReaderConfig{
		Partition: 0,
		Topic:     paymentTopic,
		Brokers:   strings.Split(broker, ","),
	})
	paymentWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(broker, ","),
		Topic:   paymentTopic,
	})

	go func() {
		for {
			select {
			case message := <-kafkaSendChannel:
				log.Printf("Sending message %v to topic %s\n", string(message.Value), message.Topic)
				var writer *kafka.Writer
				switch message.Topic {
				case userTopic:
					message.Topic = ""
					writer = userWriter
				case paymentTopic:
					message.Topic = ""
					writer = paymentWriter
				case movieTopic:
					message.Topic = ""
					writer = movieWriter
				}

				err := writer.WriteMessages(context.Background(), message)
				if err != nil {
					log.Printf("failed to write message: %s", err)
				}
			}
		}
	}()
	go func() {
		for {
			msg, err := userReader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("failed to read from user topic: %s", err)
			} else {
				userReader.CommitMessages(context.Background(), msg)
				log.Printf("Received message: %s", string(msg.Value))
			}
		}
	}()
	go func() {
		for {
			msg, err := paymentReader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("failed to read from payment topic: %s", err)
			} else {
				log.Printf("Received message: %s", string(msg.Value))
			}
		}
	}()
	go func() {
		for {
			msg, err := movieReader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("failed to read from movie topic: %s", err)
			} else {
				log.Printf("Received message: %s", string(msg.Value))
			}
		}
	}()

	return nil
}

// Movie handlers
func handleMovie(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		createMovieEvent(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func createMovieEvent(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	msg := kafka.Message{Value: data, Topic: movieTopic}
	kafkaSendChannel <- msg

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}
func handlePayment(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		createPaymentEvent(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func createPaymentEvent(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	msg := kafka.Message{Value: data, Topic: paymentTopic}
	kafkaSendChannel <- msg

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}
func handleUser(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		createUserEvent(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func createUserEvent(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	msg := kafka.Message{Value: data, Topic: userTopic}
	kafkaSendChannel <- msg

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}
