package db

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var ES *elasticsearch.Client

func ConnectDB(username string, password string) error {
	var err error
	ES, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses:     []string{"http://localhost:9200"},
		RetryOnStatus: []int{502, 503, 504, 429},
		Username:      username,
		Password:      password,
		MaxRetries:    5,
	})
	return err
}

func IndexPlaces(dataFileName string) error {
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         "places",         // The default index name
		Client:        ES,               // The Elasticsearch client
		NumWorkers:    runtime.NumCPU(), // The number of worker goroutines
		FlushBytes:    5e+6,             // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval
	})
	if err != nil {
		return err
	}

	dataFile, _ := os.Open(dataFileName)
	if err != nil {
		return err
	}
	res, err := ES.Indices.Delete([]string{"places"}, ES.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil || res.IsError() {
		return err
	}
	res.Body.Close()

	mapping := `{
		"mappings" : {
			"properties": {
			"name": {
				"type":  "text"
			},
			"address": {
				"type":  "text"
			},
			"phone": {
				"type":  "text"
			},
			"location": {
				"type": "geo_point"
			}
			}
		}
	}`
	indexReq := esapi.IndicesCreateRequest{
		Index: "places",
		Body:  strings.NewReader(mapping),
	}
	resp, err := indexReq.Do(context.Background(), ES)
	if err != nil {
		return err
	}
	resp.Body.Close()

	reader := bufio.NewReader(dataFile)
	for {
		data, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		p := &Place{}

		p.ReadFromTSV(data)

		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",

				DocumentID: strconv.Itoa(p.ID),

				Body: bytes.NewReader(p.MarshalJSON()),
			},
		)
		if err != nil {
			return err
		}
	}
	if err := bi.Close(context.Background()); err != nil {
		return err
	}
	return nil
}

type Store interface {
	// returns a list of items, a total number of hits and (or) an error in case of one
	GetPlaces(limit int, offset int) ([]Place, int, error)
	GetRecomnendation(Latitude float64, Longitude float64) ([]Place, error)
}

type Place struct {
	ID       int       `json:"id" es:"type:text"`
	Name     string    `json:"name" es:"type:text"`
	Address  string    `json:"address" es:"type:text"`
	Phone    string    `json:"phone" es:"type:text"`
	Location *GeoPoint `json:"location" es:"type:geo_point"`
}

type GeoPoint struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

func (pt *GeoPoint) Source() map[string]float64 {
	return map[string]float64{
		"lat": pt.Lat,
		"lon": pt.Lon,
	}
}

func GeoPointFromLatLon(lat, lon float64) *GeoPoint {
	return &GeoPoint{Lat: lat, Lon: lon}
}

func (p *Place) ReadFromTSV(data []byte) {
	fields := strings.Split(string(data), "	")
	p.ID, _ = strconv.Atoi(fields[0])
	p.Name = fields[1]
	p.Address = fields[2]
	p.Phone = fields[3]
	lat, _ := strconv.ParseFloat(fields[5], 64)
	lon, _ := strconv.ParseFloat(fields[4], 64)
	p.Location = GeoPointFromLatLon(lat, lon)
}

func (p *Place) MarshalJSON() []byte {
	data, err := json.Marshal(p)

	if err != nil {
		fmt.Println(err)
	}
	return data
}

func (p Place) GetPlaces(limit int, offset int) ([]Place, int, error) {
	var places []Place
	var rMap map[string]interface{}
	res, err := ES.Search(
		ES.Search.WithContext(context.Background()),
		ES.Search.WithIndex("places"),
		ES.Search.WithTrackTotalHits(true),
		ES.Search.WithPretty(),
		ES.Search.WithFrom(offset),
		ES.Search.WithSize(limit),
	)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&rMap); err != nil {
		fmt.Printf("Error parsing the response body: %s", err)
	}
	totalHits := int(rMap["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	for _, hit := range rMap["hits"].(map[string]interface{})["hits"].([]interface{}) {
		doc := hit.(map[string]interface{})
		source := doc["_source"].(map[string]interface{})
		p := Place{
			ID:      int(source["id"].(float64)),
			Name:    source["name"].(string),
			Address: source["address"].(string),
			Phone:   source["phone"].(string),
		}
		lat := (source["location"].(map[string]interface{}))["lat"].(float64)
		lon := (source["location"].(map[string]interface{}))["lon"].(float64)
		p.Location = GeoPointFromLatLon(lat, lon)
		places = append(places, p)
	}
	return places, totalHits, nil
}

func (p Place) GetRecomnendation(Latitude float64, Longitude float64) ([]Place, error) {
	var places []Place
	var rMap map[string]interface{}
	var sortBuf bytes.Buffer
	sort := map[string]interface{}{
		"sort": map[string]interface{}{
			"_geo_distance": map[string]interface{}{
				"location": map[string]interface{}{
					"lat": Latitude,
					"lon": Longitude,
				},
				"order":           "asc",
				"unit":            "km",
				"mode":            "min",
				"distance_type":   "arc",
				"ignore_unmapped": true,
			},
		},
	}
	if err := json.NewEncoder(&sortBuf).Encode(sort); err != nil {
		return nil, err
	}

	res, err := ES.Search(
		ES.Search.WithContext(context.Background()),
		ES.Search.WithBody(&sortBuf),
		ES.Search.WithIndex("places"),
		ES.Search.WithPretty(),
		ES.Search.WithSize(3),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(&rMap); err != nil {
		fmt.Printf("Error parsing the response body: %s", err)
	}
	for _, hit := range rMap["hits"].(map[string]interface{})["hits"].([]interface{}) {
		doc := hit.(map[string]interface{})
		source := doc["_source"].(map[string]interface{})
		p := Place{
			ID:      int(source["id"].(float64)),
			Name:    source["name"].(string),
			Address: source["address"].(string),
			Phone:   source["phone"].(string),
		}
		lat := (source["location"].(map[string]interface{}))["lat"].(float64)
		lon := (source["location"].(map[string]interface{}))["lon"].(float64)
		p.Location = GeoPointFromLatLon(lat, lon)
		places = append(places, p)
	}
	return places, nil
}
