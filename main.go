package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Worker represents a consumer that takes in
// messages and does processing on them. In this program it just
// makes up the messages rather than receiving them externally.
type Worker struct {
	id     int
	db     *sql.DB
	logger *log.Logger
}

func NewWorker(id int, db *sql.DB) *Worker {
	logger := log.New(os.Stdout, fmt.Sprintf("[worker_%d] ", id), 0)
	return &Worker{id: id, db: db, logger: logger}
}

func (w *Worker) run(numTasks int) {
	for i := 0; i < numTasks; i++ {
		w.logger.Println("Adding task "+fmt.Sprintf("task_%d", i), i)
		w.addTask(fmt.Sprintf("task_%d", i), i)
	}
}

// addTask simulates receiving a (possible duplicated) message
// and inserting it into the database, delaying during processing
// to simulate calling an external API
func (w *Worker) addTask(name string, number int) {
	tx, err := w.db.Begin()
	if err != nil {
		w.logger.Println("Error beginning transaction: ", err)
	}
	// If another worker is writing this row, this insert will
	// wait until the other transaction commits or rolls back
	_, err = tx.Query("INSERT INTO tasks (name, number) VALUES ($1, $2)", name, number)
	if err != nil {
		w.logger.Println("Error inserting task: ", err)
	}
	delay := rand.Intn(3)
	time.Sleep(time.Duration(delay) * time.Second)

	// Simulate a random failure of the API during the transaction
	// This can result in less-than-once processing of the message,
	// although in a Kinesis context, the consumer would not
	// mark the message as committed, so it would be re-consumed.
	// Or the API call could be retried if acceptable.
	if rand.Float32() < 0.5 {
		err = tx.Rollback()
		w.logger.Println("Rolling back task " + name)
	} else {
		// Commit the transaction, this fails if the insert
		// above failed due to the row already existing
		err = tx.Commit()
		if err != nil {
			w.logger.Println("Error while committing: ", err)
		} else {
			w.logger.Println("Finalized task " + name)
		}
	}
}

func getDBConn() *sql.DB {
	connStr := "user=postgres dbname=postgres password=password sslmode=disable port=6000"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println("DB connection error: ")
		log.Fatal(err)
	}
	return db
}

func setupTable() {
	db := getDBConn()
	_, err := db.Query("CREATE TABLE tasks (name varchar primary key , number int, created_at timestamp default now())")
	if err != nil {
		log.Fatal(err)
	}
}

func clearTable() {
	db := getDBConn()
	_, err := db.Query("DROP TABLE IF EXISTS tasks ")
	if err != nil {
		log.Fatal(err)
	}
}
func main() {
	clearTable()
	setupTable()

	var workers []*Worker
	var wg sync.WaitGroup

	var numWorkers = 30
	var messagesPerWorker = 15

	for i := 0; i < numWorkers; i++ {
		worker := NewWorker(i, getDBConn())
		workers = append(workers, worker)
	}

	// For each worker, launch a thread to
	// attempt to process messages and wait for
	// all workers to finish.
	for _, w := range workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			fmt.Println("Starting worker")
			w.run(messagesPerWorker)
		}(w)
	}
	wg.Wait()
	fmt.Println("tasks done")
}
