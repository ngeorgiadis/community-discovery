package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/ngeorgiadis/community-discovery/cmd/dominationScore/config"
	"github.com/ngeorgiadis/community-discovery/internal/domination"
)

type ExampleDatasetReader struct {
	Dimensions int
}

func main() {

	a, err := config.New("settings.json")
	if err != nil {
		panic(err)
	}

	// create timestamp and prepare output folder
	// timestamp := time.Now().Format("20060102_150405")
	// outputBasePath := path.Join(a.BaseOutputPath, timestamp)
	// err = os.MkdirAll(outputBasePath, 0777)
	// if err != nil {
	// 	panic(err.Error())
	// }

	ds := domination.New()

	dsFilePath := path.Join(a.BaseOutputPath, "domination.txt")
	reader := &ExampleDatasetReader{Dimensions: 2}
	ds.Calc(reader, a.NodesCSVFile, dsFilePath, false, a.GridSize)

	b, _ := os.ReadFile(dsFilePath)

	os.WriteFile("domination.txt", []byte(strings.TrimSpace(strings.ReplaceAll(string(b), "id	dom", ""))), 0777)

}

func (edr *ExampleDatasetReader) ReadDataset(filename string) (map[int]domination.DataRow, *domination.DataStats, []domination.DataPoint) {

	f, _ := os.Open(filename)
	r := csv.NewReader(f)
	r.Comma = '\t'

	fmt.Println("start")
	fmt.Println("reading...")

	recs, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	res := map[int]domination.DataRow{}
	stats := &domination.DataStats{
		Max:       make([]int, edr.Dimensions),
		Min:       make([]int, edr.Dimensions),
		Histogram: make([]map[int]int, edr.Dimensions),
		Count:     0,
	}

	for i := range stats.Max {
		stats.Max[i] = math.MinInt64
		stats.Min[i] = math.MaxInt64
	}

	unique := map[string]int{}

	for _, row := range recs {
		// uncomment if the csv has header row
		// if i == 0 {
		// 	continue
		// }

		attrs := make([]int, edr.Dimensions)

		id, _ := strconv.Atoi(row[0])
		v1, _ := strconv.Atoi(row[1])
		v2, _ := strconv.Atoi(row[2])
		attrs[0] = v1
		attrs[1] = v2

		// stats max
		{

			for j := range attrs {
				if attrs[j] > stats.Max[j] {
					stats.Max[j] = attrs[j]
				}
			}

			for j := range attrs {
				if attrs[j] < stats.Min[j] {
					stats.Min[j] = attrs[j]
				}
			}
		}

		// histogram
		{
			for i, a := range attrs {
				if stats.Histogram[i] == nil {
					stats.Histogram[i] = map[int]int{}
				}
				stats.Histogram[i][a]++
			}
		}

		// unique
		{
			attrKey := ""
			for i := range attrs {
				attrKey += fmt.Sprintf("%v|", attrs[i])
			}

			unique[attrKey]++
		}

		res[id] = domination.DataRow{
			ID:    id,
			Name:  fmt.Sprintf("n%v", id),
			Attrs: attrs,
		}

	}

	dataPoints := []domination.DataPoint{}

	for k, v := range unique {

		attrs := strings.Split(strings.TrimRight(k, "|"), "|")

		a := []int{}
		for i := range attrs {
			iv, _ := strconv.Atoi(attrs[i])
			a = append(a, iv)
		}

		dataPoints = append(dataPoints, domination.DataPoint{
			Count: v,
			Attrs: a,
		})
	}

	stats.Count = len(res)
	return res, stats, dataPoints

}
