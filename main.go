package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

func getDBConn() *sql.DB {
	connStr := "user=postgres dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
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
	_, err := db.Query("INSERT INTO tasks (name, number) VALUES ($1, $2)", name, number)
	if err != nil {
		log.Println(err)
	}
}

func addTasks() {
	fmt.Println("start")
	addTask("task1", 1)
	fmt.Println("1")
	addTask("task2", 2)
	fmt.Println("2")
	addTask("task3", 3)
	fmt.Println("end")
}

func main() {
	clearTable()
	setupTable()

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("adding task")
			addTasks()
		}()
	}
	wg.Wait()
	fmt.Println("tasks done")
}
