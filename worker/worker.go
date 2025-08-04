package worker

import (
	"cube/task"
	"fmt"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) CollectStats() {
	fmt.Println("collect stats")
}

func (w *Worker) RunTask() {
	fmt.Println("run task")
}

func (w *Worker) StartTask() {
	fmt.Println("start task")
}

func (w *Worker) StopTask() {
	fmt.Println("stop task")
}
