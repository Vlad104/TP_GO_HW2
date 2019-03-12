package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// функция воркер, она что-то делает
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

// функция для рассчета crc32 хэша в отдельной горутине
func crc32HashCalc(wg *sync.WaitGroup, data string, hash *string) {
	defer wg.Done()
	*hash = DataSignerCrc32(data)
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for chanData := range in {
		data := fmt.Sprint(chanData) 	// chan interface{} -> string
		md5Hash := DataSignerMd5(data)

		wg.Add(1)
		go func() {
			defer wg.Done()

			var crc32Hash1 string 		// для результата crc32(data)
			var crc32Hash2 string 		// для результата crc32(md5(data))

			localWg := &sync.WaitGroup{}
			localWg.Add(2)
			go crc32HashCalc(localWg, data, &crc32Hash1)
			go crc32HashCalc(localWg, md5Hash, &crc32Hash2)
			localWg.Wait()
			out <- crc32Hash1 + "~" + crc32Hash2
		}()
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	const th = 6						// кол-во рассчетов хэша crc32(th+data)
	var wg sync.WaitGroup
	for chanData := range in {
		go func() {
			defer wg.Done()
			data := fmt.Sprint(chanData)
			hashes := make([]string, th)
			wg.Add(1)

			localWg := &sync.WaitGroup{}
			for i := 0; i < th; i++ {
				localWg.Add(1)
				thData := strconv.Itoa(i) + data
				go crc32HashCalc(localWg, thData, &hashes[i])
			}
			localWg.Wait()
			out <- strings.Join(hashes, "")		
		}()
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	const (
		elementsMaxCount = 7
		separator = "_"
	)
	// собираем все в слайс-строк, сортируем их и соединяем
	results := make([]string, 0, elementsMaxCount)
	for chanData := range in {
		results = append(results, fmt.Sprint(chanData))
	}
	sort.Strings(results)
	out <- strings.Join(results, separator)
}