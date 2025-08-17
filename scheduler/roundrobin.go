package scheduler

import (
	"cube/node"
	"cube/task"
)

var _ Scheduler = &RoundRobin{}

type RoundRobin struct {
	Name       string
	LastWorker int
}

func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {

	nodeScores := make(map[string]float64)
	var newWorker int
	if r.LastWorker+1 < len(nodes) {
		newWorker = r.LastWorker + 1
	} else {
		newWorker = 0
	}

	r.LastWorker = newWorker

	for idx, node := range nodes {
		if idx == newWorker {
			nodeScores[node.Name] = 0.1
		} else {
			nodeScores[node.Name] = 1.0
		}
	}

	return nodeScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {

	if len(scores) == 0 || len(candidates) == 0 {
		return nil
	}

	bestNode := candidates[0]
	lowestScore := scores[bestNode.Name]

	for _, node := range candidates[1:] {
		if scores[node.Name] < lowestScore {
			bestNode = node
			lowestScore = scores[node.Name]
		}
	}

	return bestNode
}
