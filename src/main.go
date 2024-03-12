package main

import (
	"Go_Day03/TastyDiscoveries/db"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ViewData struct {
	Name     string     `json:"name"`
	Total    int        `json:"total"`
	Places   []db.Place `json:"places"`
	PrevPage int        `json:"prev_page"`
	NextPage int        `json:"next_page"`
	LastPage int        `json:"last_page"`
}

func main() {

	if err := db.ConnectDB("elastic", "YjJJgV4OoVovaH7CpCNU"); err != nil {
		fmt.Println(err)
		return
	}

	if err := db.IndexPlaces("../materials/data.csv"); err != nil {
		fmt.Println(err)
		return
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/", showPageHTML)

	mux.HandleFunc("/api/places/", showPageJSON)

	mux.HandleFunc("/api/recommend", verifyToken)

	mux.HandleFunc("/api/get_token", getToken)

	err := http.ListenAndServe(":8888", mux)
	fmt.Println(err)
}

func showPageHTML(w http.ResponseWriter, r *http.Request) {

	pageID, err := strconv.Atoi(r.URL.Query().Get("page"))
	if err != nil || pageID < 0 || pageID > 1364 {
		http.NotFound(w, r)
		return
	}
	limit := 10
	offset := pageID * limit

	gp := &db.Place{}
	places, total, _ := gp.GetPlaces(limit, offset)

	ts, err := template.ParseFiles("pages.html")
	if err != nil {
		fmt.Println(err)
	}

	data := &ViewData{
		Name:     "Places",
		Total:    total,
		Places:   places,
		PrevPage: pageID - 1,
		NextPage: pageID + 1,
		LastPage: int(total / limit),
	}

	err = ts.Execute(w, data)
	if err != nil {
		fmt.Println(err)
		http.Error(w, "Internal Server Error", 500)
	}

}

func showPageJSON(w http.ResponseWriter, r *http.Request) {

	pageID, err := strconv.Atoi(r.URL.Query().Get("page"))
	if err != nil || pageID < 0 || pageID > 1364 {
		w.WriteHeader(400)
		errJson := []byte(fmt.Sprintf("{\n    \"error\": \"Invalid 'page' value %d\"\n}", pageID))
		w.Write(errJson)
		return
	}
	limit := 10
	offset := pageID * limit

	gp := &db.Place{}
	places, total, _ := gp.GetPlaces(limit, offset)

	data := &ViewData{
		Name:     "Places",
		Total:    total,
		Places:   places,
		PrevPage: pageID - 1,
		NextPage: pageID + 1,
		LastPage: int(total / limit),
	}
	jsonData, _ := json.MarshalIndent(data, "", "    ")
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)

}

func showRecommendations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	lat, err := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, err := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	if err != nil {
		w.WriteHeader(400)
		errJson := []byte("{\n    \"error\": \"Invalid 'location' value\"\n}")
		w.Write(errJson)
		return
	}

	gp := &db.Place{}
	places, err := gp.GetRecomnendation(lat, lon)

	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("Internal server error"))
	}

	type ViewRecommendation struct {
		Name   string     `json:"name"`
		Places []db.Place `json:"places"`
	}

	data := &ViewRecommendation{
		Name:   "Recommendation",
		Places: places,
	}
	jsonData, _ := json.MarshalIndent(data, "", "    ")
	w.Write(jsonData)
}

func getToken(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	payload := jwt.MapClaims{
		"exp": time.Now().Add(time.Hour * 3).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, payload)

	t, err := token.SignedString([]byte("JWT_KEY"))
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	viewMap := map[string]string{
		"token": t,
	}
	data, _ := json.MarshalIndent(viewMap, "", "    ")
	w.Write(data)
}

func verifyToken(w http.ResponseWriter, r *http.Request) {
	authorizationHeader := r.Header.Get("Authorization")
	if authorizationHeader != "" {
		tok := strings.Split(authorizationHeader, " ")[1]
		token, err := jwt.Parse(tok, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("There was an error")
			}
			return []byte("JWT_KEY"), nil
		})
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			_, err2 := w.Write([]byte("You're Unauthorized due to error parsing the JWT"))
			if err2 != nil {
				return
			}
			return
		}
		if token.Valid {
			showRecommendations(w, r)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			_, err := w.Write([]byte("You're Unauthorized due to invalid token"))
			if err != nil {
				return
			}
		}
	} else {
		w.WriteHeader(http.StatusUnauthorized)
		_, err := w.Write([]byte("An authorization header is required"))
		if err != nil {
			return
		}
	}
}
