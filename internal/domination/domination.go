package domination

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DataRow ...
type DataRow struct {
	ID    int
	Name  string
	Attrs []int
}

type DataStats struct {
	Count int
	Max   []int
	Min   []int

	Histogram []map[int]int
}

type DataPoint struct {
	Attrs []int
	Count int
}

// DominationScoreCalculator ...
type DominationScoreCalculator struct {
}

func New() *DominationScoreCalculator {
	return &DominationScoreCalculator{}
}

func a_equals_b(a, b []int) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func a_greater_or_equal_to_b(a, b []int) bool {
	for i := range a {
		if a[i] < b[i] {
			return false
		}
	}
	return true
}

// a_dominates_b function need to be optimized
// to perform best
func a_dominates_b(a, b []int) bool {

	// if (a[0] == b[0]) &&
	// 	(a[1] == b[1]) &&
	// 	(a[2] == b[2]) &&
	// 	(a[3] == b[3]) {
	// 	return false
	// }

	// return ((a[0] >= b[0]) &&
	// 	(a[1] >= b[1]) &&
	// 	(a[2] >= b[2]) &&
	// 	(a[3] >= b[3]))

	if a_equals_b(a, b) {
		return false
	}

	return a_greater_or_equal_to_b(a, b)
}

func a_less_b(a, b []int) bool {
	// return ((a[0] < b[0]) &&
	// 	(a[1] < b[1]) &&
	// 	(a[2] < b[2]) &&
	// 	(a[3] < b[3]))

	for i := range a {
		if a[i] >= b[i] {
			return false
		}
	}
	return true
}

func a_less_or_equal_b(a, b []int) bool {
	// return ((a[0] <= b[0]) &&
	// 	(a[1] <= b[1]) &&
	// 	(a[2] <= b[2]) &&
	// 	(a[3] <= b[3]))
	for i := range a {
		if a[i] > b[i] {
			return false
		}
	}
	return true
}

func getKey(a []int) string {
	res := ""
	for i := range a {
		res += fmt.Sprintf("%v|", a[i])
	}
	return res
}

func translate(p []int, stats *DataStats, gridSize ...int) []int {

	res := make([]int, len(p))

	for i := range p {
		steps := float64(stats.Max[i]-stats.Min[i]) / float64(gridSize[i])
		n, _ := math.Modf(float64(p[i]) / steps)
		res[i] = int(n)
		// res[i] = int(float64(p[i]) / steps)
	}

	return res
}

func translate2(p []int, stats *DataStats, gridSize ...int) float64 {

	// res := make([]float64, len(p))

	res := 1.0

	for i := range p {
		steps := float64(stats.Max[i]-stats.Min[i]) / float64(gridSize[i])
		_, f := math.Modf(float64(p[i]) / steps)
		res = res * f
	}

	return res
}

func sumSlice(n []int) int {
	s := 0
	for i := range n {
		s += n[i]
	}
	return s
}

type DatasetReader interface {
	ReadDataset(filename string) (map[int]DataRow, *DataStats, []DataPoint)
}

type DefaultDatasetReader struct{}

// type DominationChecker interface {
// 	Dominates(a, b []int) bool
// }

// type DefaultDominationChecker struct{}

// func (ddc *DefaultDominationChecker) Dominates(a, b []int) bool {
// 	if (a[0] == b[0]) &&
// 		(a[1] == b[1]) &&
// 		(a[2] == b[2]) &&
// 		(a[3] == b[3]) {
// 		return false
// 	}

// 	return (a[0] >= b[0]) &&
// 		(a[1] >= b[1]) &&
// 		(a[2] >= b[2]) &&
// 		(a[3] >= b[3])
// }

// ReadDatasetRows reads the csv file and returns
// a. the data in a map[int]DataRow structure
// b. a DataStats structure
// c. a slice with all unique data points
func (ddr *DefaultDatasetReader) ReadDataset(filename string) (map[int]DataRow, *DataStats, []DataPoint) {
	f, _ := os.Open(filename)
	r := csv.NewReader(f)

	fmt.Println("start")
	fmt.Println("reading...")

	recs, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	res := map[int]DataRow{}
	stats := &DataStats{
		Max:       make([]int, 4),
		Min:       make([]int, 4),
		Histogram: make([]map[int]int, 4),
		Count:     0,
	}

	stats.Max = []int{math.MinInt64, math.MinInt64, math.MinInt64, math.MinInt64}
	stats.Min = []int{math.MaxInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64}

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

		attrs := []int{
			pc, cn, hi, int(pi),
		}

		// stats max
		{
			if pc > stats.Max[0] {
				stats.Max[0] = pc
			}

			if cn > stats.Max[1] {
				stats.Max[1] = cn
			}

			if hi > stats.Max[2] {
				stats.Max[2] = hi
			}

			if int(pi) > stats.Max[3] {
				stats.Max[3] = int(pi)
			}

			// stats min
			if pc < stats.Min[0] {
				stats.Min[0] = pc
			}

			if cn < stats.Min[1] {
				stats.Min[1] = cn
			}

			if hi < stats.Min[2] {
				stats.Min[2] = hi
			}

			if int(pi) < stats.Min[3] {
				stats.Min[3] = int(pi)
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

		res[id] = DataRow{
			ID:    id,
			Name:  row[1],
			Attrs: attrs,
		}

	}

	dataPoints := []DataPoint{}

	for k, v := range unique {

		attrs := strings.Split(strings.TrimRight(k, "|"), "|")

		a := []int{}
		for i := range attrs {
			iv, _ := strconv.Atoi(attrs[i])
			a = append(a, iv)
		}

		dataPoints = append(dataPoints, DataPoint{
			Count: v,
			Attrs: a,
		})
	}

	stats.Count = len(res)
	return res, stats, dataPoints
}

func datapointSortFn(data []DataPoint) func(i, j int) bool {
	return func(i, j int) bool {
		a1 := data[i]
		a2 := data[j]

		s1 := 0
		for i1 := range a1.Attrs {
			s1 += a1.Attrs[i1]
		}

		s2 := 0
		for i2 := range a2.Attrs {
			s2 += a2.Attrs[i2]
		}

		avg1 := float64(s1) / float64(len(a1.Attrs))
		sd1 := 0.0
		for i1 := range a1.Attrs {
			sd1 += math.Pow(avg1-float64(a1.Attrs[i1]), 2.0)
		}

		avg2 := float64(s2) / float64(len(a2.Attrs))
		sd2 := 0.0
		for i2 := range a2.Attrs {
			sd2 += math.Pow(avg2-float64(a2.Attrs[i2]), 2.0)
		}

		if s1 > s2 {
			return true
		} else if s1 == s2 {
			return sd1 < sd2
		} else {
			return false
		}
	}
}

func (dsc *DominationScoreCalculator) Calc(dataReader DatasetReader, inputFile string, outputFile string, approximate bool, gridSize []int) {
	total := time.Now()

	t1 := time.Now()
	rows, stats, unique := dataReader.ReadDataset(inputFile)
	fmt.Printf("reading done in: %v\n", time.Since(t1))

	t1 = time.Now()
	sort.Slice(unique, datapointSortFn(unique))

	domination := map[string]int{}
	grid := map[string][]DataPoint{}

	// split to grid
	for i := range unique {

		c := unique[i]

		coordinates := translate(c.Attrs, stats, gridSize...)
		key := getKey(coordinates)

		if _, ok := grid[key]; !ok {
			grid[key] = []DataPoint{}
		}

		grid[key] = append(grid[key], c)
	}

	// get sorted coords
	gridCoors := []DataPoint{}
	for k := range grid {

		coords := strings.Split(strings.TrimRight(k, "|"), "|")

		a := []int{}
		for i := range coords {
			iv, _ := strconv.Atoi(coords[i])
			a = append(a, iv)
		}

		gridCoors = append(gridCoors, DataPoint{
			Attrs: a,
		})

	}
	fmt.Printf("creating grid done in: %v\n", time.Since(t1))

	// main loop
	mainCalc := time.Now()
	t1 = time.Now()
	sort.Slice(gridCoors, datapointSortFn(gridCoors))

	var la time.Duration
	var lb time.Duration
	var lc time.Duration

	for i := range gridCoors {

		ik := getKey(gridCoors[i].Attrs)
		point := gridCoors[i].Attrs

		sum := sumSlice(point)

		baseScore := 0
		later := []DataPoint{}

		l1 := time.Now()

		for _, j := range gridCoors[i:] {
			if sumSlice(j.Attrs) > sum {
				continue
			}

			jk := getKey(j.Attrs)
			point_to_compare_with := j.Attrs

			if a_less_b(point_to_compare_with, point) {
				for _, v := range grid[jk] {
					baseScore += v.Count
				}
			} else if a_less_or_equal_b(point_to_compare_with, point) {
				later = append(later, grid[jk]...)
			}
		}
		la += time.Since(l1)

		for _, n := range grid[ik] {

			nodeScore := baseScore

			if approximate {
				l2 := time.Now()
				agrCellItems := 0
				for _, l := range later {
					agrCellItems += l.Count
				}

				apprx := translate2(n.Attrs, stats, gridSize...)
				approximateScore := float64(agrCellItems) * apprx

				nodeScore += int(approximateScore)
				lb += time.Since(l2)

			} else {

				l3 := time.Now()
				for _, l := range later {
					if a_dominates_b(n.Attrs, l.Attrs) {
						nodeScore += l.Count
					}
				}
				lc += time.Since(l3)
			}

			domination[getKey(n.Attrs)] = nodeScore
		}

		if i%1000 == 0 && i > 0 {
			fmt.Printf("%v (%v of %v)\t%v\t%v\t%v\tapprx:%v\n", time.Since(t1), i+1, len(gridCoors), la, lb, lc, approximate)
			t1 = time.Now()
		}
	}
	fmt.Printf("main calc done in: %v\n", time.Since(mainCalc))
	t1 = time.Now()

	// write outfile
	fd, err := os.Create(outputFile)
	if err != nil {
		fd, _ = os.Create("dom_out_new.txt")
	}

	fd.WriteString("id\tdom\n")
	for i := range rows {
		n := rows[i]
		k := getKey(n.Attrs)
		fd.WriteString(fmt.Sprintf("%v\t%v\n", n.ID, domination[k]))
	}
	fd.Close()

	fmt.Printf("write results to file done in: %v\n", time.Since(t1))
	fmt.Println(time.Since(total))
}
