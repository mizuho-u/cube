package manager

import (
	"bytes"
	"cube/node"
	"cube/scheduler"
	"cube/store"
	"cube/task"
	"cube/worker"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

func New(workers []string, schedulerType, dbType string) (*Manager, error) {
	taskWorkerMap := make(map[uuid.UUID]string)

	workerTaskMap := make(map[string][]uuid.UUID)
	var nodes []*node.Node
	for _, worker := range workers {
		workerTaskMap[worker] = []uuid.UUID{}

		nAPI := fmt.Sprintf("http://%v", worker)
		n := node.New(worker, nAPI, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "roundrobin":
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	m := &Manager{
		Penging:       *queue.New(),
		Workers:       workers,
		TaskWorkerMap: taskWorkerMap,
		WorkerTaskMap: workerTaskMap,
		WorkerNodes:   nodes,
		Scheduler:     s,
	}

	var ts store.Store[*task.Task]
	var es store.Store[*task.TaskEvent]
	var err error
	switch dbType {
	case "memory":
		ts = store.NewInMemoryTaskStore[*task.Task]()
		es = store.NewInMemoryTaskStore[*task.TaskEvent]()
	case "persistent":
		ts, err = store.NewPersistentTaskStore[*task.Task]("tasks.db", 0600, "tasks")
		if err != nil {
			return nil, err
		}

		es, err = store.NewPersistentTaskStore[*task.TaskEvent]("events.db", 0600, "events")
		if err != nil {
			return nil, err
		}
	}

	m.TaskDb = ts
	m.EventDb = es

	return m, nil

}

type Manager struct {
	Penging       queue.Queue
	TaskDb        store.Store[*task.Task]
	EventDb       store.Store[*task.TaskEvent]
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := m.logln("No available candidates match resource request for task %v", t.ID)
		err := errors.New(msg)
		return nil, err
	}
	scores := m.Scheduler.Score(t, candidates)
	sectedNode := m.Scheduler.Pick(scores, candidates)

	return sectedNode, nil
}

func (m *Manager) updateTasks() {

	for _, worker := range m.Workers {
		m.logln("Checking worker %v for task updates", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			m.logln("Error connecting to %v: %v", worker, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			m.logln("Error sending request %v", err)
			continue
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			m.logln("Error unmarshalling tasks: %s", err.Error())
			continue
		}

		for _, t := range tasks {
			m.logln("Attempting to update task %v", t.ID)

			taskPersisted, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				m.logln("Task with ID %s not found", t.ID)
				return
			}

			if taskPersisted.State != t.State {
				taskPersisted.State = t.State
			}

			taskPersisted.StartTime = t.StartTime
			taskPersisted.FinishTime = t.FinishTime
			taskPersisted.ContainerID = t.ContainerID
			taskPersisted.HostPorts = t.HostPorts
		}

	}

	m.logln("Update task")
}

func (m *Manager) SendWork() {

	if m.Penging.Len() > 0 {

		e := m.Penging.Dequeue()
		te := e.(task.TaskEvent)
		err := m.EventDb.Put(te.ID.String(), &te)
		if err != nil {
			m.logln("Error attempting to store task event %s: %s\n", te.ID.String(), err)
			return
		}
		m.logln("Pulled %v off pending queue", te)

		taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
		if ok {
			persistedTask, err := m.TaskDb.Get(te.Task.ID.String())
			if err != nil {
				m.logln("unable to schedule task: %s", err)
				return
			}

			if te.State == task.Completed && task.ValidStateTransition(persistedTask.State, te.State) {
				m.stopTask(taskWorker, te.Task.ID.String())
				return
			}

			m.logln("Invalid rquest: existing task %s is in state %v and cannot transition to the completed state", persistedTask.ID.String(), persistedTask.State)
			return
		}

		t := te.Task
		w, err := m.SelectWorker(t)
		if err != nil {
			m.logln("Error selecting worker for task %s: %v", t.ID, err)
			return
		}

		m.logln("Select worker %s", w.Name)

		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], te.Task.ID)
		m.TaskWorkerMap[t.ID] = w.Name

		t.State = task.Scheduled
		m.TaskDb.Put(t.ID.String(), &t)

		data, err := json.Marshal(te)
		if err != nil {
			m.logln("Unable to marshal task object: %v.", t)
		}

		url := fmt.Sprintf("http://%s/tasks", w.Name)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			m.logln("Error connecting to %v: %v", w.Name, err)
			m.Penging.Enqueue(te)
			return
		}

		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {

			e := worker.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				m.logln("Error decoding to %s", err)
				return
			}

			m.logln("Response error (%d): %s", e.HTTPStatusCode, e.Message)
			return
		}

		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			m.logln("Error decoding response %s", err)
			return
		}

		t.ScheduledOn = w.Name
		m.logln("%#v", t)
	} else {
		m.logln("No Work in the queue")
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Penging.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {

	tasks, err := m.TaskDb.List()
	if err != nil {
		m.logln("error getting list of tasks: %v\n", err)
		return nil
	}

	return tasks
}

func (m *Manager) UpdateTasks() {
	for {
		m.logln("Checking for task updates from workers")
		m.updateTasks()
		m.logln("Task updates completed")
		m.logln("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		m.logln("Proccessing any tasks in the queue")
		m.SendWork()
		m.logln("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) DoHealthChecks() {
	for {
		m.logln("Performing task health check")
		m.doHealthChecks()
		m.logln("Task health checks completed")
		m.logln("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	m.logln("Calling health check for task %s: %s", t.ID, t.HealthCheck)
	w := m.TaskWorkerMap[t.ID]
	hostport := getHostPort(t.HostPorts)
	if hostport == nil {
		m.logln("Hostport is empty")
		return nil
	}

	worker := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostport, t.HealthCheck)

	m.logln("Calling health check for task %s: %s", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		m.logln(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200", t.ID)
		m.logln(msg)
		return errors.New(msg)
	}

	m.logln("Task %s health check response: %v", t.ID, resp.StatusCode)

	return nil
}

func getHostPort(ports nat.PortMap) *string {
	for k := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {

		if t.RestartCount > 3 {
			continue
		}

		switch t.State {
		case task.Running:
			if err := m.checkTaskHealth(*t); err != nil {
				m.restartTask(t)
			}
		case task.Failed:
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {

	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb.Put(t.ID.String(), t)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(te)
	if err != nil {
		m.logln("Unable to marshal task object: %v.", t)
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		m.logln("Error connecting to %v: %v", w, err)
		m.Penging.Enqueue(te)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {

		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			m.logln("Error decoding to %s", err)
			return
		}

		m.logln("Response error (%d): %s", e.HTTPStatusCode, e.Message)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		m.logln("Error decoding response %s", err)
		return
	}

	m.logln("%#v", newTask)

}

func (m *Manager) stopTask(worker, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		m.logln("Error creating request to delete task %s: %v", taskID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		m.logln("Error connecting worker at %s: %v", url, err)
		return
	}

	if resp.StatusCode != 204 {
		m.logln("Error sending request: %v", err)
		return
	}

	m.logln("Task %s has been scheduled to be stopped", taskID)
}

func (m *Manager) logln(msg string, param ...any) string {

	s := "[manager] " + msg
	if len(param) >= 1 {
		s = fmt.Sprintf(s, param...)
	}

	log.Println(s)

	return s
}
