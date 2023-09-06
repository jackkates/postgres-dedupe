package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func getDBConn() *sql.DB {
	connStr := "user=postgres dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Println("db conn error")
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
	_, err := db.Query("DROP TABLE tasks")
	if err != nil {
		log.Fatal(err)
	}
}

func addTask(name string, number int) {
	db := getDBConn()
	tx, err := db.Begin()
	_, err = tx.Query("INSERT INTO tasks (name, number) VALUES ($1, $2)", name, number)
	if err != nil {
		fmt.Println("query error: ", err)
	}
	delay := rand.Intn(5)
	time.Sleep(time.Duration(delay) * time.Second)
	if rand.Float32() < 0.5 {
		err = tx.Rollback()
		fmt.Println("rollback task " + name)
	} else {
		err = tx.Commit()
		if err != nil {
			fmt.Println("commit error: ", err)
		} else {
			fmt.Println("finalized task " + name)
		}
	}
}

func addTasks(numTasks int) {
	for i := 0; i < numTasks; i++ {
		addTask(fmt.Sprintf("task_%d", i), i)
	}
}

func main() {
	clearTable()
	setupTable()

	var wg sync.WaitGroup

	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("adding task")
			addTasks(10)
		}()
	}
	wg.Wait()
	fmt.Println("tasks done")
}
