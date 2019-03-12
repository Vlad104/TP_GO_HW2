package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func worker(wg *sync.WaitGroup, task job, in, out chan interface{}) {
	defer wg.Done()		// перед выходом уменьшим счетчик группы
	task(in, out)		// что-то делаем и пишем в канал результат
	close(out)			// закрываем канал, тк все записали
}

func ExecutePipeline(tasks ...job) {
	// создаем буферезированный канал
	current := make(chan interface{}, MaxInputDataLen)

	wg := &sync.WaitGroup{}					// инициализируем группу
	for _, task := range tasks {
		// создаем буферезированный канал для записи
		next := make(chan interface{}, MaxInputDataLen)
		wg.Add(1)							// добавляем воркер
		go worker(wg, task, current, next)
		current = next
	}
	wg.Wait()	// ждем, пока счетчик групп не уменьшится до 0
}

func SingleHash(in, out chan interface{}) {
	for input := range in {
		data := fmt.Sprint(input)
		md5Hash := DataSignerMd5(data)

		crc32Hash1 := DataSignerCrc32(data)
		crc32Hash2 := DataSignerCrc32(md5Hash)
		out <- crc32Hash1 + "~" + crc32Hash2
	}
}

func stepMultiHash(hashes *string, data string, index int) {
	*hashes = strconv.Itoa(index) + data
	*hashes = DataSignerCrc32(*hashes)
}

func MultiHash(in, out chan interface{}) {
	const hashCount = 6
	for input := range in {
		hashes := make([]string, hashCount)
		data := fmt.Sprint(input)
		for i := 0; i < hashCount; i++ {
			stepMultiHash(&hashes[i], data, i)
		}
		out <- strings.Join(hashes, "")
	}
}

func CombineResults(in, out chan interface{}) {
	elementsMaxCount := 10
	results := make([]string, 0, elementsMaxCount)
	for result := range in {
		results = append(results, fmt.Sprint(result))
	}
	sort.Strings(results)
	out <- strings.Join(results, "_")
}