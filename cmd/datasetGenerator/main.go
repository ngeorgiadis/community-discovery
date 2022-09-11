package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ngeorgiadis/community-discovery/cmd/datasetGenerator/config"
	"github.com/ngeorgiadis/community-discovery/internal/domination"
)

func correlated(mean float64, d int) []int {
	res := make([]int, d)
	for i := range res {

		nf := rand.NormFloat64()
		for nf < 0 {
			nf = rand.NormFloat64()
		}

		res[i] = int(nf*25 + mean)
	}
	return res
}

func uniform(d int) []int {
	res := make([]int, d)
	for i := range res {
		res[i] = int(rand.Int31n(255))
	}
	return res
}

func main() {

	a, err := config.New("settings.json")
	if err != nil {
		panic(err)
	}

	suffix := "exact"
	if a.Approximate {
		suffix = "approx"
	}

	datasetFilename := fmt.Sprintf("dataset_%v_%v.txt", a.DatasetType, time.Now().Format("20060102_150405"))
	outputFile := fmt.Sprintf("domination_%v_%v_%v.txt", a.DatasetType, time.Now().Format("20060102_150405"), suffix)
	outputPath := path.Join(a.BaseOutputPath, outputFile)

	err = os.MkdirAll(a.BaseOutputPath, 0777)
	if err != nil {
		panic(err.Error())
	}

	f, _ := os.Create(datasetFilename)
	for i := 0; i < a.DatasetSize; i++ {

		v := make([]int, a.DatasetDimensions)

		switch a.DatasetType {
		case "UNIFORM":
			v = uniform(a.DatasetDimensions)
		case "CORRELATED":
			r := rand.NormFloat64()
			for r < 0 {
				r = rand.NormFloat64()
			}
			v = correlated(float64(r*50), a.DatasetDimensions)
		}

		vs := ""
		for _, vi := range v {
			vs += fmt.Sprintf("%v\t", vi)
		}
		vs = strings.TrimSpace(vs)
		f.WriteString(fmt.Sprintf("%v\t%v\n", i, vs))

		if i%10000 == 0 {
			fmt.Print("+")
		}
	}
	f.Close()
	fmt.Println(".")

	ds := domination.New()
	syntheticReader := &SyntheticDatasetReader{
		Dimensions: a.DatasetDimensions,
	}

	ds.Calc(syntheticReader, datasetFilename, outputPath, a.Approximate, a.GridSize)
}

type SyntheticDatasetReader struct {
	Dimensions int
}

func (sdr *SyntheticDatasetReader) ReadDataset(filename string) (map[int]domination.DataRow, *domination.DataStats, []domination.DataPoint) {
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
		Max:       make([]int, sdr.Dimensions),
		Min:       make([]int, sdr.Dimensions),
		Histogram: make([]map[int]int, sdr.Dimensions),
		Count:     0,
	}

	for i := range stats.Max {
		stats.Max[i] = math.MinInt64
		stats.Min[i] = math.MaxInt64
	}

	// stats.Max = []int{math.MinInt64, math.MinInt64, math.MinInt64, math.MinInt64}
	// stats.Min = []int{math.MaxInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64}

	unique := map[string]int{}

	for i, row := range recs {
		if i == 0 {
			continue
		}

		attrs := make([]int, sdr.Dimensions)

		var id int

		for it, cell := range row {
			v, _ := strconv.Atoi(cell)
			if it == 0 {
				id, _ = strconv.Atoi(row[0])
				continue
			}

			attrs[it-1] = v
		}

		// pc, _ := strconv.Atoi(row[1])
		// cn, _ := strconv.Atoi(row[2])
		// hi, _ := strconv.Atoi(row[3])
		// pi, _ := strconv.Atoi(row[4])

		// attrs := []int{
		// 	pc, cn, hi, pi,
		// }

		// stats max / min
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

	fmt.Println(len(unique))

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
