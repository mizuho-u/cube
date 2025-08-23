package task

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

func (s State) String() string {

	var str string
	switch s {
	case Pending:
		str = "Pending"
	case Scheduled:
		str = "Scheduled"
	case Running:
		str = "Running"
	case Completed:
		str = "Completed"
	case Failed:
		str = "Failed"
	}

	return str
}

var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Scheduled, Running, Failed},
	Running:   {Running, Completed, Failed},
	Completed: {Completed},
	Failed:    {Scheduled},
}

func Contains(states []State, state State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}

	return false
}

func ValidStateTransition(src State, dst State) bool {
	return Contains(stateTransitionMap[src], dst)
}
