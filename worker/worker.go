package worker

import (
	"cube/store"
	"cube/task"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        store.Store[*task.Task]
	TaskCount int
	Stats     *Stats
}

func New(name string, taskDbType string) (*Worker, error) {
	w := Worker{
		Name:  name,
		Queue: *queue.New(),
	}

	var s store.Store[*task.Task]
	var err error
	switch taskDbType {
	case "memory":
		s = store.NewInMemoryTaskStore[*task.Task]()
	case "persistent":
		s, err = store.NewPersistentTaskStore[*task.Task](fmt.Sprintf("%s_tasks.db", name), 0600, "tasks")
	}

	if err != nil {
		return nil, err
	}

	w.Db = s
	return &w, nil
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

	tasks, err := w.Db.List()
	if err != nil {
		w.Logln("error getting list of tasks: %v\n", err)
		return nil
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

	err := w.Db.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("error storing task %s: %v", taskQueued.ID.String(), err)
		w.Logln("%s", msg)
		return task.DockerResult{Error: msg}
	}

	taskPersisted, err := w.Db.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("error getting task %s from database: %v", taskQueued.ID.String(), err)
		w.Logln("%s", msg)
		return task.DockerResult{Error: msg}
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
		w.Db.Put(t.ID.String(), &t)
		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running
	w.Db.Put(t.ID.String(), &t)

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
	w.Db.Put(t.ID.String(), &t)

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

	tasks, err := w.Db.List()
	if err != nil {
		w.Logln("error getting list of tasks: %v\n", err)
		return
	}

	for id, t := range tasks {

		if t.State == task.Running {
			resp := w.InspecTask(*t)
			if resp.Error != nil {
				w.Logln("Error: %v", resp.Error)
				continue
			}

			if resp.Container == nil {
				w.Logln("No container for running task %s", id)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			if resp.Container.State.Status == "exited" {
				w.Logln("Container for task %s in non-running state %s", id, resp.Container.State.Status)
				t.State = task.Failed
				w.Db.Put(t.ID.String(), t)
			}

			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			w.Db.Put(t.ID.String(), t)
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
