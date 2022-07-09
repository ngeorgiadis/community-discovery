package config

import (
	"encoding/json"
	"io/ioutil"
)

type AppConfig struct {
	DatasetType       string `json:"datasetType"`
	DatasetSize       int    `json:"datasetSize"`
	DatasetDimensions int    `json:"datasetDimensions"`
	BaseOutputPath    string `json:"baseOutputPath"`
	GridSize          []int  `json:"gridSize"`
}

func New(configFile string) (*AppConfig, error) {

	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	c := AppConfig{}
	err = json.Unmarshal(b, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}
