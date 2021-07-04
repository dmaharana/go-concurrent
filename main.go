package main

// https://brunoscheufler.com/blog/2019-09-21-parallelized-batch-processing-in-go

import (
	// "fmt"
	"log"
	"sync"
	"time"
)

func main() {
	items := generateIntSlice(20)

	const maxBatchSize int = 5

	respSlice := callInBatches(items, maxBatchSize)

	log.Println(respSlice)
}

func callInBatches(items []int, maxBatchSize int) []int {
	var (
		skip                = 0
		filesAmount         = len(items)
		batchAmount         = int(float64(filesAmount / maxBatchSize))
		successItems        = make([]int, 0)
		failItems           = make([]int, 0)
		itemProcessingGroup sync.WaitGroup
	)

	for i := 0; i <= batchAmount; i++ {
		lowerBound := skip
		upperBound := skip + maxBatchSize

		if upperBound > filesAmount {
			upperBound = filesAmount
		}

		batchItems := items[lowerBound:upperBound]

		skip += maxBatchSize

		itemProcessingGroup.Add(len(batchItems))

		for idx := range batchItems {
			go func(currentItem *int, currentIdx int) {
				// notify the WaitGroup of the completion
				defer itemProcessingGroup.Done()
				k, r := myroutine(*currentItem)
				switch r {
				case "SUCCESS":
					successItems = append(successItems, k)
				case "FAIL":
					failItems = append(failItems, k)
				}
			}(&batchItems[idx], idx)
		}

		itemProcessingGroup.Wait()

	}

	log.Println(successItems, failItems)

	return failItems
}

func myroutine(keyvalue int) (int, string) {
	var (
		ctime = time.Now().Unix()
		r     string
	)
	const processTime = 3

	log.Printf("%d:Working with %d\n", ctime, keyvalue)

	time.Sleep(time.Duration(processTime) * time.Second)

	// ctime = time.Now().Unix()

	if keyvalue%2 == 0 {
		// r = fmt.Sprintf("%d:%d:SUCCESS", ctime, keyvalue)
		r = "SUCCESS"
	} else {
		// r = fmt.Sprintf("%d:%d:FAIL", ctime, keyvalue)
		r = "FAIL"
	}

	log.Println("Completed: ", keyvalue, ":", r)

	return keyvalue, r
}

func generateIntSlice(sliceSize int) []int {
	intSlice := make([]int, 0)

	for i := 0; i < sliceSize; i++ {
		intSlice = append(intSlice, i)
	}

	return intSlice
}
