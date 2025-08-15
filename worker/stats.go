package worker

import (
	"log"

	"github.com/c9s/goprocinfo/linux"
)

func GetStats() *Stats {
	return &Stats{
		MemStats:  getMemoryInfo(),
		DiskStats: getDiskInfo(),
		CpuStats:  getCpuStats(),
		LoadStats: getLoadAverage(),
	}
}

type Stats struct {
	MemStats  *linux.MemInfo
	DiskStats *linux.Disk
	CpuStats  *linux.CPUStat
	LoadStats *linux.LoadAvg
	TaskCount int
}

func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.MemTotal
}

func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.MemAvailable
}

func (s *Stats) MemUsedKb() uint64 {
	return s.MemStats.MemTotal - s.MemStats.MemAvailable
}

func (s *Stats) MemUsedPercent() uint64 {
	return s.MemStats.MemAvailable / s.MemStats.MemTotal
}

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.All
}

func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

func (s *Stats) CpuUsage() float64 {
	idle := s.CpuStats.Idle + s.CpuStats.IOWait
	nonIdle := s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System + s.CpuStats.IRQ + s.CpuStats.SoftIRQ + s.CpuStats.Steal
	total := idle + nonIdle

	if total == 0 {
		return 0.00
	}

	return (float64(total) - float64(idle)) / float64(total)
}

func getMemoryInfo() *linux.MemInfo {

	if memstats, err := linux.ReadMemInfo("/proc/meminfo"); err != nil {
		log.Printf("Error reading from /proc/meminfo")
		return &linux.MemInfo{}
	} else {
		return memstats
	}
}

func getDiskInfo() *linux.Disk {

	if diskstats, err := linux.ReadDisk("/"); err != nil {
		log.Printf("Error reading from /")
		return &linux.Disk{}
	} else {
		return diskstats
	}
}

func getCpuStats() *linux.CPUStat {

	if cpustats, err := linux.ReadStat("/proc/stat"); err != nil {
		log.Printf("Error reading from /proc/stat")
		return &linux.CPUStat{}
	} else {
		return &cpustats.CPUStatAll
	}
}

func getLoadAverage() *linux.LoadAvg {

	if memstats, err := linux.ReadLoadAvg("/proc/loadavg"); err != nil {
		log.Printf("Error reading from /proc/loadavg")
		return &linux.LoadAvg{}
	} else {
		return memstats
	}
}
