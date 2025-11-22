package knn

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"pcd-pc4/pkg/network"
	"sort"
	"strconv"
)

// ---------------------------------------------------------
// Estructuras reutilizables por API, nodos ML y ejecutables
// ---------------------------------------------------------

type Rating struct {
	UserID  string
	MovieID string
	Rating  float64
}

type Neighbor struct {
	UserID     string
	Similarity float64
}

type NeighborResult struct {
	UserID     string
	Similarity float64
}

type Recommended struct {
	MovieID   string
	Predicted float64
}

// ---------------------------------------------------------
// Carga de datos (igual que antes, pero como funciones públicas)
// ---------------------------------------------------------

func LoadUserRatings(path string) map[string]map[string]float64 {
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

func LoadMovieTitles(path string) map[string]string {
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

// ---------------------------------------------------------
// Similitud de Coseno  (usado por KNN local y nodos TCP)
// ---------------------------------------------------------

func CosineSimilarity(a, b map[string]float64) float64 {
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

// ---------------------------------------------------------
// Ordenamiento y selección de los K mejores vecinos
// ---------------------------------------------------------

func TopK(list []network.NeighborResult, k int) []network.NeighborResult {
	sort.Slice(list, func(i, j int) bool {
		return list[i].Similarity > list[j].Similarity
	})
	if len(list) > k {
		return list[:k]
	}
	return list
}

// ---------------------------------------------------------
// Generar recomendaciones finales a partir de vecinos K
// ---------------------------------------------------------

func PredictRatings(target string, ratings map[string]map[string]float64, neighbors []network.NeighborResult) []Recommended {
	targetRatings := ratings[target]

	scoreSum := make(map[string]float64)
	weightSum := make(map[string]float64)

	for _, nb := range neighbors {
		ratings := ratings[nb.UserID]

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
		recs = append(recs, Recommended{
			MovieID:   movie,
			Predicted: s / w,
		})
	}

	return recs
}

// ---------------------------------------------------------
// Top N recomendaciones ordenadas
// ---------------------------------------------------------

func TopNRecommendations(recs []Recommended, n int) []Recommended {
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Predicted > recs[j].Predicted
	})
	if len(recs) > n {
		return recs[:n]
	}
	return recs
}

// ---------------------------------------------------------
// Guardado de resultados en CSV (para ejecutables)
// ---------------------------------------------------------

func SaveNeighborsCSV(path, target string, neighbors []NeighborResult) {
	f, _ := os.Create(path)
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"TargetUser", "NeighborUser", "Similarity"})
	for _, n := range neighbors {
		w.Write([]string{target, n.UserID, fmt.Sprintf("%.6f", n.Similarity)})
	}
}

func SaveRecommendationsCSV(path, target string, recs []Recommended, titles map[string]string) {
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
