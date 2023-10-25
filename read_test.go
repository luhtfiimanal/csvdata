package csvdata_test

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"testing"
)

func BenchmarkReadCSVLineByLine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		csvfile, _ := os.Open("example/2023-01-10.csv")
		defer csvfile.Close()

		reader := csv.NewReader(csvfile)

		for {
			_, err := reader.Read()
			if err != nil {
				break
			}
		}
	}
}

func BenchmarkReadCSVAllAtOnce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		csvfile, _ := os.Open("example/2023-01-10.csv")
		defer csvfile.Close()

		reader := csv.NewReader(csvfile)
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadCSVSequentiallyAllAtOnce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j <= 1; j++ {
			csvfile, _ := os.Open(fmt.Sprintf("example/2023-01-1%d.csv", j))
			defer csvfile.Close()

			reader := csv.NewReader(csvfile)
			_, _ = reader.ReadAll()
		}
	}
}
func BenchmarkReadCSVConcurrentlyAllAtOnce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(2)

		for j := 0; j <= 1; j++ {
			go func(j int) {
				csvfile, _ := os.Open(fmt.Sprintf("example/2023-01-1%d.csv", j))
				defer csvfile.Close()

				reader := csv.NewReader(csvfile)
				_, _ = reader.ReadAll()

				wg.Done()
			}(j)
		}

		wg.Wait()
	}
}

func BenchmarkReadCSVConcurrentlyLineByLine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(2)

		for j := 0; j <= 1; j++ {
			go func(j int) {
				csvfile, _ := os.Open(fmt.Sprintf("example/2023-01-1%d.csv", j))
				defer csvfile.Close()

				reader := csv.NewReader(csvfile)

				for {
					_, err := reader.Read()
					if err != nil {
						break
					}
				}

				wg.Done()
			}(j)
		}

		wg.Wait()
	}
}
