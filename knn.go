package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Rating struct {
	UserID  string
	MovieID string
	Rating  float64
}

type Neighbor struct {
	UserID     string
	Similarity float64
}

type Recommended struct {
	MovieID   string
	Predicted float64
}

const (
	K              = 50  // número de vecinos más cercanos
	TopN           = 10  // cantidad de recomendaciones por usuario
	MaxUsersSample = 100 // número de usuarios para prueba de speedup
)

var workerCountsToTest = []int{1, 2, 4, 8, 16}

func main() {
	fmt.Println("Iniciando User-Based Collaborative Filtering con Cosine Similarity...")
	os.MkdirAll("recommendation", os.ModePerm)

	userRatings := loadUserRatings("data/clean/ratings.csv")
	movieTitles := loadMovieTitles("data/clean/movies.csv")

	var users []string
	for u := range userRatings {
		users = append(users, u)
	}
	sort.Strings(users)
	if len(users) == 0 {
		fmt.Println("No se encontraron usuarios.")
		return
	}

	sampleSize := MaxUsersSample
	if len(users) < sampleSize {
		sampleSize = len(users)
	}
	sampleUsers := users[:sampleSize]

	speedupRecords := [][]string{{"Workers", "ElapsedSeconds"}}
	for _, workers := range workerCountsToTest {
		start := time.Now()
		runKNNForUsersParallel(userRatings, movieTitles, sampleUsers, workers, K)
		elapsed := time.Since(start).Seconds()
		speedupRecords = append(speedupRecords, []string{strconv.Itoa(workers), fmt.Sprintf("%.6f", elapsed)})
		fmt.Printf("Workers=%d completado en %.3fs\n", workers, elapsed)
	}

	saveCSV("recommendation/speedup.csv", speedupRecords)
	fmt.Println("Proceso completado. Resultados guardados en /recommendation/")
}

// -------------------- CARGA DE DATOS --------------------

func loadUserRatings(path string) map[string]map[string]float64 {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("Error al abrir", path, ":", err)
		return nil
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, _ := r.ReadAll()
	userRatings := make(map[string]map[string]float64)

	for i, rec := range records {
		if i == 0 || len(rec) < 3 {
			continue
		}
		rating, err := strconv.ParseFloat(rec[2], 64)
		if err != nil {
			continue
		}
		user := rec[0]
		movie := rec[1]
		if _, ok := userRatings[user]; !ok {
			userRatings[user] = make(map[string]float64)
		}
		userRatings[user][movie] = rating
	}
	return userRatings
}

func loadMovieTitles(path string) map[string]string {
	f, err := os.Open(path)
	if err != nil {
		return map[string]string{}
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, _ := r.ReadAll()
	m := make(map[string]string)
	for i, rec := range records {
		if i == 0 || len(rec) < 2 {
			continue
		}
		m[rec[0]] = rec[1]
	}
	return m
}

// -------------------- PROCESAMIENTO PRINCIPAL --------------------

func runKNNForUsersParallel(userRatings map[string]map[string]float64, movieTitles map[string]string, targetUsers []string, workers int, k int) {
	for _, target := range targetUsers {
		neighbors := computeNeighborsParallel(target, userRatings, workers, k)
		saveNeighborsCSV("recommendation/neighbors_user_"+sanitizeFilename(target)+".csv", target, neighbors)

		recs := generateRecommendations(target, userRatings, neighbors, TopN)
		saveRecommendationsCSV("recommendation/recommendations_user_"+sanitizeFilename(target)+".csv", target, recs, movieTitles)
	}
}

// -------------------- CÁLCULO DE VECINOS --------------------

func computeNeighborsParallel(target string, userRatings map[string]map[string]float64, workers int, k int) []Neighbor {
	var others []string
	for u := range userRatings {
		if u != target {
			others = append(others, u)
		}
	}

	jobs := make(chan string, 1000)
	results := make(chan Neighbor, 1000)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range jobs {
				sim := cosineSimilarity(userRatings[target], userRatings[v])
				if sim > 0 {
					results <- Neighbor{UserID: v, Similarity: sim}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	go func() {
		for _, v := range others {
			jobs <- v
		}
		close(jobs)
	}()

	var all []Neighbor
	for n := range results {
		all = append(all, n)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Similarity > all[j].Similarity
	})
	if len(all) > k {
		all = all[:k]
	}
	return all
}

func cosineSimilarity(a, b map[string]float64) float64 {
	var dot, normA, normB float64
	for item, ra := range a {
		if rb, ok := b[item]; ok {
			dot += ra * rb
		}
		normA += ra * ra
	}
	for _, rb := range b {
		normB += rb * rb
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// -------------------- GENERAR RECOMENDACIONES --------------------

func generateRecommendations(target string, userRatings map[string]map[string]float64, neighbors []Neighbor, topN int) []Recommended {
	targetRatings := userRatings[target]
	scoreSum := make(map[string]float64)
	weightSum := make(map[string]float64)

	for _, nb := range neighbors {
		ratings := userRatings[nb.UserID]
		for movie, r := range ratings {
			if _, seen := targetRatings[movie]; seen {
				continue
			}
			scoreSum[movie] += nb.Similarity * r
			weightSum[movie] += math.Abs(nb.Similarity)
		}
	}

	var recs []Recommended
	for movie, s := range scoreSum {
		w := weightSum[movie]
		if w == 0 {
			continue
		}
		pred := s / w
		recs = append(recs, Recommended{MovieID: movie, Predicted: pred})
	}

	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Predicted > recs[j].Predicted
	})
	if len(recs) > topN {
		recs = recs[:topN]
	}
	return recs
}

// -------------------- CSV --------------------

func saveNeighborsCSV(path, target string, neighbors []Neighbor) {
	f, _ := os.Create(path)
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"TargetUser", "NeighborUser", "Similarity"})
	for _, n := range neighbors {
		w.Write([]string{target, n.UserID, fmt.Sprintf("%.6f", n.Similarity)})
	}
}

func saveRecommendationsCSV(path, target string, recs []Recommended, titles map[string]string) {
	f, _ := os.Create(path)
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"TargetUser", "MovieID", "Title", "PredictedRating"})
	for _, r := range recs {
		title := titles[r.MovieID]
		w.Write([]string{target, r.MovieID, title, fmt.Sprintf("%.4f", r.Predicted)})
	}
}

func saveCSV(path string, rows [][]string) {
	f, _ := os.Create(path)
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	for _, row := range rows {
		w.Write(row)
	}
}

// -------------------- UTILIDADES --------------------

func sanitizeFilename(s string) string {
	out := strings.ReplaceAll(s, "/", "_")
	out = strings.ReplaceAll(out, "\\", "_")
	return out
}
