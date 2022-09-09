# Twin

Command line tool for finding duplicate files on your system and reporting the results back.

## Command Line Options

`-recursive`: to scan a directory and all of its subdirectories, defaults to false.

`-worker_count`: number of goroutines to read files content.

`-directory`: absolute path to a directory to scan, defaults to the current directory.

`-max_size`: the maximum size in megabytes a file can be to be included in the scan, defaults to 15 megabytes.