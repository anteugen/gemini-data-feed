package main

import (
	"log"
	"encoding/json"
	"os"
    "os/signal"
    "syscall"
    "github.com/gorilla/websocket"
	"time"
)

type Event struct {
    Delta     string `json:"delta"`
    Price     string `json:"price"`
    Reason    string `json:"reason"`
    Remaining string `json:"remaining"`
    Side      string `json:"side"`
    Type      string `json:"type"`
}

type Message struct {
    EventID       int64   `json:"eventId"`
    Events        []Event `json:"events"`
    SocketSequence int    `json:"socket_sequence"`
    Timestamp     int64   `json:"timestamp"`
    TimestampMs   int64   `json:"timestampms"`
    Type          string  `json:"type"`
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	url := "wss://api.gemini.com/v1/marketdata/BTCUSD?top_of_book=false&bids=true"

	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		log.Println("error:", err)
	}
	defer conn.Close()

	log.Println("Connected to Gemini WebSocket API for BTCUSD market data")

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
                log.Println("read:", err)
                return
            }

			var parsedMessage Message

			err = json.Unmarshal(message, &parsedMessage)
        	if err != nil {
            	log.Println("Error parsing JSON message:", err)
            	continue
        	}
		
			for _, event := range parsedMessage.Events {
				log.Printf("%s %s", event.Price, event.Remaining)
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case <-interrupt:
			log.Println("Interrupt, shutting down")
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
            if err != nil {
                log.Println("write close:", err)
                return
            }
			select {
            	case <-done:
            	case <-time.After(1 * time.Second):
					log.Println("Shutdown timeout, exiting")
            }
            return
		}
	}
}