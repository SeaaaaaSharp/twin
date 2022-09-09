package main

import (
	"crypto/md5"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

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

func parseArgs() (int64, int64, bool, string) {
	var maxSize *int64 = flag.Int64("max_size", 15, "maximum file size should be positive integer")
	var directory *string = flag.String("directory", "./", "absolute path required")
	var workerCount *int64 = flag.Int64("worker_count", 10, "number of workers should be a positive integer")
	var isRecursive *bool = flag.Bool("recursive", false, "recursive is used to indicate whether files in subdirectories should be included")

	flag.Parse()

	if *workerCount < 1 {
		log.Fatal("number of workers should be a positive integer")
	}

	if *maxSize < 1 {
		log.Fatal("maximum file size should be positive integer")
	}

	return *workerCount, ((*maxSize) * megabyte), *isRecursive, *directory
}

func listDirectory(directory string, maxSize int64, filePathChannel chan string) {
	if files, err := ioutil.ReadDir(directory); exitOnError(err) {
		for _, fileInfo := range files {
			notSymlink := !(fileInfo.Mode() & os.ModeSymlink != 0)

			if notSymlink && (fileInfo.IsDir() == false) && (maxSize > fileInfo.Size()) {
				absolutePath := filepath.Join(directory, fileInfo.Name())
				filePathChannel <- absolutePath
			}
		}
	}
}

func listDirectoryRecursively(directory string, maxSize int64, filePathChannel chan string) {
	filepath.Walk(directory, func(absolutePath string, fileInfo os.FileInfo, err error) error {
		exitOnError(err)

		notSymlink := !(fileInfo.Mode() & os.ModeSymlink != 0)

		if notSymlink && (fileInfo.IsDir() == false) && (maxSize > fileInfo.Size()) {
			filePathChannel <- absolutePath
		}

		return nil
	})
}

func dispatchFilePaths(directory string, maxSize int64, isRecursive bool, filePathChannel chan string) {
	if isRecursive {
		listDirectoryRecursively(directory, maxSize, filePathChannel)
		return
	}
	listDirectory(directory, maxSize, filePathChannel)
}

func hashFile(filePath string) [16]byte {
	fileContent, err := os.ReadFile(filePath)
	exitOnError(err)
	return md5.Sum(fileContent)
}

func listenForFilePath(filePathChannel chan string) {
	for filePath := range filePathChannel {
		hashValue := hashFile(filePath)
		mutexForResults.Lock()
		results[hashValue] = append(results[hashValue], filePath)
		mutexForResults.Unlock()
	}
	wg.Done()
}

func reportDuplicates() {
	totalFileCount := 0

	duplicateCount := 0

	for _, filePaths := range results {

		if len(filePaths) > 1 {
			log.Println("Duplicates: ", filePaths)
			duplicateCount++
		}

		totalFileCount += len(filePaths)
	}
	log.Println("Total files scanned: ", totalFileCount, " . Found ", duplicateCount, " duplicates.")
}

func main() {
	workerCount, maxSize, isRecursive, directory := parseArgs()

	log.Println("Scanning: ", directory, " . Using ", workerCount, " workers")

	filePathChannel := make(chan string, workerCount)

	wg.Add(int(workerCount))

	for i := int64(0); i < workerCount; i++ {
		go listenForFilePath(filePathChannel)
	}

	dispatchFilePaths(directory, maxSize, isRecursive, filePathChannel)

	close(filePathChannel)

	wg.Wait()

	reportDuplicates()
}
