package main

import (
	"crypto/md5"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	// "time"
	"fmt"
	"sync"
)

var _ = fmt.Println

const (
	megabyte = 1048576
)

var (
	results         = make(map[[16]byte][]string)
	mutexForResults = sync.RWMutex{}
)

var wg sync.WaitGroup

func exitOnError(err error) bool {
	if err != nil {
		log.Fatal("encountered error:", err)
	}
	return true
}

func parseArgs() (uint, int64, bool, string) {
	var maxSize *int64 = flag.Int64("max_size", 15, "maximum file size should be positive integer")
	var directory *string = flag.String("directory", "./", "absolute path required")
	var workerCount *uint = flag.Uint("worker_count", 10, "workers count should be positive integer")
	var isRecursive *bool = flag.Bool("recursive", false, "recursive is used to indicate whether files in subdirectories should be included")

	flag.Parse()

	if *workerCount == 0 {
		log.Fatal("workers count should be a positive integer")
	}

	if *maxSize < 1 {
		log.Fatal("maximum file size should be positive integer")
	}

	return *workerCount, ((*maxSize) * megabyte), *isRecursive, *directory
}

func listDirectory(directory string, maxSize int64) []string {
	var includedFiles []string

	if files, err := ioutil.ReadDir(directory); exitOnError(err) {
		for _, fileInfo := range files {
			// SKIP SIMLINKS HERE
			if (fileInfo.IsDir() == false) && (maxSize > fileInfo.Size()) {
				absolutePath := filepath.Join(directory, fileInfo.Name())
				includedFiles = append(includedFiles, absolutePath)
			}
		}
	}

	return includedFiles
}

func listDirectoryRecursively(directory string, maxSize int64) []string {
	var includedFiles []string

	filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		exitOnError(err)

		// SKIP SIMLINKS HERE
		if (info.IsDir() == false) && (maxSize > info.Size()) {
			includedFiles = append(includedFiles, path)
		}

		return nil
	})

	return includedFiles
}

func listFiles(directory string, maxSize int64, isRecursive bool) []string {
	if isRecursive {
		return listDirectoryRecursively(directory, maxSize)
	}
	return listDirectory(directory, maxSize)
}

func hashFile(filePath string) [16]byte {
	fileContent, err := os.ReadFile(filePath)
	exitOnError(err)
	return md5.Sum(fileContent)
}

func listenForFilePath(filePathChannel chan string) {
	for {
		select {
		case filePath := <-filePathChannel:
			hashValue := hashFile(filePath)
			mutexForResults.Lock()
			results[hashValue] = append(results[hashValue], filePath)
			mutexForResults.Unlock()
		default:
			wg.Done()
			return
		}
	}
}

func reportDuplicates() {
	for _, filePaths := range results {
		if len(filePaths) > 1 {
			fmt.Println("Duplicates -> ", filePaths)
		}
	}
}

func main() {
	workerCount, maxSize, isRecursive, directory := parseArgs()

	filesToHash := listFiles(directory, maxSize, isRecursive)

	filePathChannel := make(chan string, len(filesToHash))

	wg.Add(int(workerCount))

	for i := 0; i < int(workerCount); i++ {
		go listenForFilePath(filePathChannel)
	}

	for _, filePath := range filesToHash {
		filePathChannel <- filePath
	}

	wg.Wait()

	close(filePathChannel)

	reportDuplicates()
}

