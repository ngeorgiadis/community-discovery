package config

import (
	"encoding/json"
	"io/ioutil"
)

type AppConfig struct {
	NodesCSVFile   string `json:"nodesCSVFile"`
	EdgesCSVFile   string `json:"edgesCSVFile"`
	BaseOutputPath string `json:"baseOutputPath"`
	GridSize       []int  `json:"gridSize"`
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
