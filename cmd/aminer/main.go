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
	"time"

	"github.com/ngeorgiadis/community-discovery/cmd/aminer/config"
	"github.com/ngeorgiadis/community-discovery/internal/domination"
)

func main() {
	// max 572, 15757, 60, 8308
	// gridSize := []int{25, 25, 25, 25}
	// gridSize := []int{
	// 	10, 10, 10, 10,
	// }

	a, err := config.New("settings.json")
	if err != nil {
		panic(err)
	}

	// create timestamp and prepare output folder
	timestamp := time.Now().Format("20060102_150405")
	outputBasePath := path.Join(a.BaseOutputPath, timestamp)
	err = os.MkdirAll(outputBasePath, 0777)
	if err != nil {
		panic(err.Error())
	}

	ds := domination.New()
	dsFilePath := path.Join(outputBasePath, "domination.txt")

	defaultReader := &AminerDatasetReader{Dimensions: a.Dimensions}

	ds.Calc(defaultReader, a.NodesCSVFile, dsFilePath, a.Approximate, a.GridSize)
}

type AminerDatasetReader struct {
	Dimensions int
}

func (adr *AminerDatasetReader) ReadDataset(filename string) (map[int]domination.DataRow, *domination.DataStats, []domination.DataPoint) {
	f, _ := os.Open(filename)
	r := csv.NewReader(f)

	fmt.Println("start")
	fmt.Println("reading...")

	recs, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	res := map[int]domination.DataRow{}
	stats := &domination.DataStats{
		Max:       make([]int, adr.Dimensions),
		Min:       make([]int, adr.Dimensions),
		Histogram: make([]map[int]int, adr.Dimensions),
		Count:     0,
	}

	for i := range stats.Max {
		stats.Max[i] = math.MinInt64
		stats.Min[i] = math.MaxInt64
	}

	unique := map[string]int{}

	for i, row := range recs {
		if i == 0 {
			continue
		}

		id, _ := strconv.Atoi(row[0])

		pc, _ := strconv.Atoi(row[2])
		cn, _ := strconv.Atoi(row[3])
		hi, _ := strconv.Atoi(row[4])
		pi, _ := strconv.ParseFloat(row[5], 64)
		pi = math.Trunc(pi)

		attrs := make([]int, adr.Dimensions)

		switch adr.Dimensions {
		case 2:
			attrs[0] = pc
			attrs[1] = cn
		case 3:
			attrs[0] = pc
			attrs[1] = cn
			attrs[2] = hi
		case 4:
			attrs[0] = pc
			attrs[1] = cn
			attrs[2] = hi
			attrs[3] = int(pi)
		default:
			panic("dataset dimensions should be 2, 3 or 4")
		}

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
			Name:  row[1],
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
