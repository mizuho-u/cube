package node

import "cube/worker"

type Node struct {
	Name            string
	Ip              string
	Cores           int
	Memory          int
	MemoryAllocated int
	Disk            int
	DiskAllocated   int
	Role            string
	TaskCount       int
	Stats           worker.Stats
}

func New(worker, address, role string) *Node {
	return &Node{
		Name: worker,
		Ip:   address,
		Role: role,
	}
}
