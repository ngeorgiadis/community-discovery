package main

import (
	"os"
	"path"
	"time"

	"github.com/ngeorgiadis/community-discovery/cmd/dominationScore/config"
	"github.com/ngeorgiadis/community-discovery/internal/domination"
)

func main() {

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
	defaultReader := &domination.DefaultDatasetReader{}

	// max 572, 15757, 60, 8308
	// gridSize := []int{25, 25, 25, 25}
	// gridSize := []int{
	// 	10, 10, 10, 10,
	// }

	ds.Calc(defaultReader, a.NodesCSVFile, dsFilePath, true, a.GridSize)
}
