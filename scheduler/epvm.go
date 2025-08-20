package scheduler

import (
	"cube/node"
	"cube/task"
	"math"
	"time"
)

var _ Scheduler = &Epvm{}

type Epvm struct {
	Name string
}

func (e *Epvm) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for _, node := range nodes {
		if checkDisk(t, node.Disk-node.DiskAllocated) {
			candidates = append(candidates, node)
		}
	}

	return candidates
}

func checkDisk(t task.Task, diskAvailable int) bool {
	return t.Disk <= diskAvailable
}

const (
	LIEB = 1.53960071783900203869
)

func (e *Epvm) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	maxJobs := 4.0

	for _, node := range nodes {
		cpuUsage := calculateCpuUsage(node)
		cpuLoad := calculateLoad(cpuUsage, math.Pow(2, 0.8))

		memoryAllocated := float64(node.Stats.MemUsedKb()) + float64(node.MemoryAllocated)
		memoryPercentAllocated := memoryAllocated / float64(node.Memory)

		newMemPercent := (calculateLoad(memoryAllocated+float64(t.Memory/1000), float64(node.Memory)))

		memCost := math.Pow(LIEB, newMemPercent)
		memCost += math.Pow(LIEB, float64(node.TaskCount+1)/maxJobs)
		memCost -= math.Pow(LIEB, memoryPercentAllocated)
		memCost -= math.Pow(LIEB, float64(node.TaskCount)/float64(maxJobs))

		cpuCost := math.Pow(LIEB, cpuLoad)
		cpuCost += math.Pow(LIEB, float64(node.TaskCount+1)/maxJobs)
		cpuCost -= math.Pow(LIEB, cpuLoad)
		cpuCost -= math.Pow(LIEB, float64(node.TaskCount)/float64(maxJobs))

		nodeScores[node.Name] = memCost + cpuCost
	}

	return nodeScores
}

func (e *Epvm) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {

	if len(candidates) == 0 {
		return nil
	}

	if len(candidates) == 1 {
		return candidates[0]
	}

	bestNode := candidates[0]
	minCost := scores[bestNode.Name]

	for _, node := range candidates[1:] {
		if scores[node.Name] < minCost {
			minCost = scores[node.Name]
			bestNode = node
		}
	}

	return bestNode
}

func calculateCpuUsage(n *node.Node) float64 {

	stat1 := node.GetStats(n)
	time.Sleep(3 * time.Second)
	stat2 := node.GetStats(n)

	stat1Idle := stat1.CpuStats.Idle + stat1.CpuStats.IOWait
	stat2Idle := stat2.CpuStats.Idle + stat2.CpuStats.IOWait

	stat1NonIdle := stat1.CpuStats.User + stat1.CpuStats.Nice + stat1.CpuStats.System + stat1.CpuStats.IRQ + stat1.CpuStats.SoftIRQ + stat1.CpuStats.Steal
	stat2NonIdle := stat2.CpuStats.User + stat2.CpuStats.Nice + stat2.CpuStats.System + stat2.CpuStats.IRQ + stat2.CpuStats.SoftIRQ + stat2.CpuStats.Steal

	stat1Total := stat1Idle + stat1NonIdle
	stat2Total := stat2Idle + stat2NonIdle

	total := stat2Total - stat1Total
	idle := stat2Idle - stat1Idle

	var cpuPercentUsage float64
	if total == 0 && idle == 0 {
		cpuPercentUsage = 0.00
	} else {
		cpuPercentUsage = (float64(total) - float64(idle)) / float64(total)
	}

	return cpuPercentUsage
}

func calculateLoad(usage, capacity float64) float64 {
	return usage / capacity
}
