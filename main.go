package main

import (
	"cube/manager"
	"cube/worker"
	"fmt"
	"os"
	"strconv"
)

func main() {

	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	fmt.Println("Starting Cube worker")

	w1, err := worker.New("w1", "persistent")
	if err != nil {
		return
	}
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: w1}

	w2, err := worker.New("w2", "persistent")
	if err != nil {
		return
	}
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: w2}

	w3, err := worker.New("w3", "persistent")
	if err != nil {
		return
	}
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: w3}

	go w1.RunTasks()
	go w1.CollectStats()
	go w1.UpdateTasks()
	go wapi1.Start()

	go w2.RunTasks()
	go w2.CollectStats()
	go w2.UpdateTasks()
	go wapi2.Start()

	go w3.RunTasks()
	go w3.CollectStats()
	go w3.UpdateTasks()
	go wapi3.Start()

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}
	m, err := manager.New(workers, "epvm", "persistent")
	if err != nil {
		return
	}
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()
}
