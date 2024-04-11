package main

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"fmt"
	"database/sql"
    _ "github.com/lib/pq"
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
	EventID        int64   `json:"eventId"`
	Events         []Event `json:"events"`
	SocketSequence int     `json:"socket_sequence"`
	Timestamp      int64   `json:"timestamp"`
	TimestampMs    int64   `json:"timestampms"`
	Type           string  `json:"type"`
}

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	url := "wss://api.gemini.com/v1/marketdata/BTCUSD?top_of_book=false&bids=true"

	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		log.Println("error:", err)
		return
	}
	defer conn.Close()

	log.Println("Connected to Gemini WebSocket API for BTCUSD market data")

	db, err := sql.Open("postgres", "user=hank password=123456 dbname=gemini sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)

		var latestBid, latestAsk Event
		var lastLoggedBid, lastLoggedAsk Event

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
				if event.Side == "bid" {
					latestBid = event
				} else if event.Side == "ask" {
					latestAsk = event
				}
			}
	
			if (latestBid != lastLoggedBid) ||
			   (latestAsk != lastLoggedAsk) {
				fmt.Printf("%s %s - %s %s\n",
					latestBid.Price, latestBid.Remaining, latestAsk.Price, latestAsk.Remaining)

				_, err := db.Exec("INSERT INTO marketdata (bestbid, quantitybid, bestask, quantityask) VALUES ($1, $2, $3, $4)", latestBid.Price, latestBid.Remaining, latestAsk.Price, latestAsk.Remaining)
				if err != nil {
					log.Fatal(err)
				}
	
				lastLoggedBid = latestBid
				lastLoggedAsk = latestAsk
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
