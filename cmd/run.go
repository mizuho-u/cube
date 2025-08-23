/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a new task",
	Long: `cube run command.

The run command starts a new task`,
	Run: func(cmd *cobra.Command, args []string) {
		manager, _ := cmd.Flags().GetString("manager")
		filename, _ := cmd.Flags().GetString("filename")

		fullFilePath, err := filepath.Abs(filename)
		if err != nil {
			log.Println(err)
			return
		}

		if !fileExists(fullFilePath) {
			log.Printf("file %s not exists", fullFilePath)
			return
		}

		log.Printf("Using manager: %v\n", manager)
		log.Printf("Using file: %v\n", fullFilePath)

		data, err := os.ReadFile(filename)
		if err != nil {
			log.Println(err)
			return
		}

		url := fmt.Sprintf("http://%s/tasks", manager)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Println(err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			log.Printf("Error sending request: %v", resp.StatusCode)
			return
		}
		log.Println("Successfully sent task request to manager")
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().StringP("manager", "m", "localhost:5555", "Manger to talk to")
	runCmd.Flags().StringP("filename", "f", "task.json", "Task specification file")

}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)

	return !errors.Is(err, fs.ErrNotExist)
}
