package main

import (
	"crypto/md5"
	"flag"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	megabyte = 1 << 20
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

func shouldBeIncluded(fileInfo fs.FileInfo, maxSize int64) bool {
	notSymlink := !(fileInfo.Mode()&os.ModeSymlink != 0)

	isIncluded := (fileInfo.IsDir() == false) && (maxSize >= fileInfo.Size())

	return notSymlink && isIncluded
}

func listDirectory(directory string, maxSize int64, filePathChannel chan string) {
	if files, err := ioutil.ReadDir(directory); exitOnError(err) {
		for _, fileInfo := range files {
			if shouldBeIncluded(fileInfo, maxSize) {
				absolutePath := filepath.Join(directory, fileInfo.Name())
				filePathChannel <- absolutePath
			}
		}
	}
}

func listDirectoryRecursively(directory string, maxSize int64, filePathChannel chan string) {
	filepath.WalkDir(directory, func(absolutePath string, entry fs.DirEntry, err error) error {
		exitOnError(err)

		fileInfo, err := entry.Info()

		exitOnError(err)

		if shouldBeIncluded(fileInfo, maxSize) {
			filePathChannel <- absolutePath
		}

		return nil
	})
}

func dispatchFilePaths(directory string, maxSize int64, isRecursive bool, filePathChannel chan string) {
	if isRecursive {
		listDirectoryRecursively(directory, maxSize, filePathChannel)
	} else {
		listDirectory(directory, maxSize, filePathChannel)
	}
}

func hashFile(filePath string) [16]byte {
	fileReader, err := os.Open(filePath)

	exitOnError(err)

	defer fileReader.Close()

	hasher := md5.New()

	_, err = io.Copy(hasher, fileReader)

	exitOnError(err)

	return *(*[16]byte)(hasher.Sum(nil))
}

func storeHashValue(hashValue [16]byte, filePath string) {
	mutexForResults.Lock()
	results[hashValue] = append(results[hashValue], filePath)
	mutexForResults.Unlock()
}

func listenForFilePath(filePathChannel chan string) {
	for filePath := range filePathChannel {
		hashValue := hashFile(filePath)
		storeHashValue(hashValue, filePath)
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
	log.Println("Total files scanned:", totalFileCount, ". Duplicates found:", duplicateCount)
}

func main() {
	workerCount, maxSize, isRecursive, directory := parseArgs()

	log.Println("Scanning: ", directory, ". Using", workerCount, "workers")

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
