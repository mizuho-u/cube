package node

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
}

func New(worker, address, role string) *Node {
	return &Node{
		Name: worker,
		Ip:   address,
		Role: role,
	}
}
