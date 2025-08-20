package node

import (
	"cube/util"
	"cube/worker"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func GetStats(n *Node) *worker.Stats {

	var resp *http.Response
	var err error

	url := fmt.Sprintf("%s/stats", n.Ip)
	resp, err = util.HTTPWithRetry(http.Get, url)
	if err != nil {
		log.Printf("Error connecting to %v: %s", n.Ip, err)
		return nil
	}

	if resp.StatusCode != 200 {
		log.Printf("Error retrieving stats from %v: %d", n.Ip, resp.StatusCode)
		return nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var stats worker.Stats

	if err := json.Unmarshal(body, &stats); err != nil {
		log.Printf("Error unmarshalling stats %v: %s", n.Ip, err)
		return nil
	}

	n.Memory = int(stats.MemTotalKb())
	n.Disk = int(stats.DiskTotal())

	n.Stats = stats

	return &stats

}
