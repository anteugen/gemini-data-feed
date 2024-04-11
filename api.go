package main

import (
    "fmt"
    "net/http"
	"encoding/json"
    "database/sql"
    "strconv"
    _ "github.com/lib/pq"
    "log"
)

type Greeting struct {
    Message string `json:"message"`
}

func connectToDB() *sql.DB {
    connStr := "user=hank password=123456 dbname=gemini sslmode=disable"
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        log.Fatal("Error: ", err)
    }
    return db
}

func fetchData(db *sql.DB, limit int) ([][]string, error) {
    query := "SELECT bidprice, bidremaining, askprice, askremaining FROM marketdata ORDER BY id DESC LIMIT $1"
    rows, err := db.Query(query, limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var data [][]string
    for rows.Next() {
        var bestbid, quantitybid, bestask, quantityask string
        err := rows.Scan(&bestbid, &quantitybid, &bestask, &quantityask)
        if err != nil {
            return nil, err
        }
        record := []string{bestbid, quantitybid, bestask, quantityask}
        data = append(data, record)
    }

    return data, nil
}

func main() {
    http.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, "Hello, World!\n")
    })

    db := connectToDB()
    defer db.Close()

	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
        limitStr := r.URL.Query().Get("limit")
        limit := 100
        if limitStr != "" {
            var err error
            limit, err = strconv.Atoi(limitStr)
            if err != nil {
                http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
                return
            }
        }
        if limit > 500 {
            limit = 500
        }
		data, err := fetchData(db, limit)
        if err != nil {
            log.Fatal("Error: ", err)
            return
        }
        jsonData, err := json.Marshal(data)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
		w.Header().Set("Content-type", "application/json")
		w.Write(jsonData)
	})

    fmt.Println("Server is running on http://localhost:8080")
    http.ListenAndServe(":8080", nil)
}
