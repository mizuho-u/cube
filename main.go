package main

import (
	"cube/task"
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/client"
)

func main() {

	dockerTask, createResult := createContainer()
	if createResult.Error != nil {
		os.Exit(1)
	}

	time.Sleep(time.Second * 5)
	stopContainer(dockerTask, createResult.ContainerId)
}

func createContainer() (*task.Docker, *task.DockerResult) {
	c := task.Config{
		Name:  "test-container-1",
		Image: "postgres:13",
		Env: []string{
			"POSTGRES_USER=cube",
			"POSTGRES_PASSWORD=secret",
		},
	}

	dc, _ := client.NewClientWithOpts(client.FromEnv)
	d := task.Docker{
		Client: dc,
		Config: c,
	}

	result := d.Run()
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return nil, &result
	}

	fmt.Printf("container is running %s\n", result.ContainerId)
	return &d, &result
}

func stopContainer(d *task.Docker, id string) *task.DockerResult {

	result := d.Stop(id)
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return &result
	}

	fmt.Printf("container has been stoped and removed %s\n", id)
	return &result

}
