package config

import (
	"encoding/json"
	"io/ioutil"
)

type AppConfig struct {
	NodesCSVFile   string `json:"nodesCSVFile"`
	EdgesCSVFile   string `json:"edgesCSVFile"`
	BaseOutputPath string `json:"baseOutputPath"`
	Dimensions     int    `json:"dimensions"`
	GridSize       []int  `json:"gridSize"`
	Approximate    bool   `json:"approximate"`
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
