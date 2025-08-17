package worker

import (
	"cube/task"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
	Stats     *Stats
}

func (w *Worker) CollectStats() {
	for {
		w.Logln("Collecting stats")
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) GetTasks() []*task.Task {

	tasks := []*task.Task{}
	for _, t := range w.Db {
		tasks = append(tasks, t)
	}

	return tasks
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) runTask() task.DockerResult {

	t := w.Queue.Dequeue()
	if t == nil {
		w.Logln("no task in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)

	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = &taskQueued
	}

	var result task.DockerResult
	if task.ValidStateTransition(
		taskPersisted.State, taskQueued.State,
	) {
		switch taskQueued.State {
		case task.Scheduled:
			result = w.StartTask(taskQueued)
		case task.Completed:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("we should not get here")
		}
	} else {
		result.Error = fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
	}

	return result
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	result := d.Run()
	if result.Error != nil {
		w.Logln("error staring container %s", result.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		w.Logln("error stopping container %s", result.Error)
		return result
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t

	w.Logln("stopped and removed container %s for %s", t.ContainerID, t.ID)
	return result
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.runTask()
			if result.Error != nil {
				w.Logln("Error running task: %v", result.Error)
			} else {
				w.Logln("No tasks to process currently.")
			}
		}

		w.Logln("Sleep for 10 seconds.")
		time.Sleep(10 * time.Second)

	}
}

func (w *Worker) InspecTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		w.Logln("Checking status of tasks")
		w.updateTasks()
		w.Logln("Task updated competed")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	for id, t := range w.Db {

		if t.State == task.Running {
			resp := w.InspecTask(*t)
			if resp.Error != nil {
				w.Logln("Error: %v", resp.Error)
				continue
			}

			if resp.Container == nil {
				w.Logln("No container for running task %s", id)
				w.Db[id].State = task.Failed
			}

			if resp.Container.State.Status == "exited" {
				w.Logln("Container for task %s in non-running state %s", id, resp.Container.State.Status)
				w.Db[id].State = task.Failed
			}

			w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
		}

	}
}

func (w *Worker) Logln(msg string, param ...any) string {

	s := "[worker " + w.Name + "] " + msg
	if len(param) >= 1 {
		s = fmt.Sprintf(s, param...)
	}

	log.Println(s)

	return s
}
