package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

const workerCount = 8 // número de goroutines concurrentes

type Rating struct {
	UserID  string
	MovieID string
	Rating  float64
}

type Movie struct {
	MovieID string
	Title   string
	Genres  string
}

type Tag struct {
	UserID    string
	MovieID   string
	Tag       string
	Timestamp string
}

func main() {
	fmt.Println("Iniciando análisis avanzado del MovieLens limpio...")

	os.MkdirAll("analisis", os.ModePerm)

	ratings := loadRatings("data/clean/ratings.csv")
	movies := loadMovies("data/clean/movies.csv")
	tags := loadTags("data/clean/tags.csv")

	analyzeRatings(ratings)
	analyzeTopMovies(ratings, movies)
	analyzeTags(tags)
	analyzeMovieAverages(ratings, movies)
	analyzeGenres(ratings, movies)
	generateUserMovieMatrix(ratings, 1000)

	fmt.Println("Análisis completo. Archivos guardados en carpeta /analisis")
}

// ---------------------- CARGA DE CSV ----------------------

func loadRatings(path string) []Rating {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error al abrir", path, ":", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()

	var ratings []Rating
	for i, rec := range records {
		if i == 0 {
			continue // header
		}
		r, err := strconv.ParseFloat(rec[2], 64)
		if err != nil {
			continue
		}
		ratings = append(ratings, Rating{rec[0], rec[1], r})
	}
	return ratings
}

func loadMovies(path string) map[string]Movie {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error al abrir", path, ":", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()
	movies := make(map[string]Movie)
	for i, rec := range records {
		if i == 0 {
			continue
		}
		movies[rec[0]] = Movie{rec[0], rec[1], rec[2]}
	}
	return movies
}

func loadTags(path string) []Tag {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error al abrir", path, ":", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, _ := reader.ReadAll()

	var tags []Tag
	for i, rec := range records {
		if i == 0 {
			continue
		}
		tags = append(tags, Tag{rec[0], rec[1], rec[2], rec[3]})
	}
	return tags
}

// ---------------------- ANÁLISIS DE RATINGS ----------------------

func analyzeRatings(ratings []Rating) {
	counts := make(map[float64]int)
	userSet := make(map[string]bool)
	movieSet := make(map[string]bool)

	var mu sync.Mutex
	var wg sync.WaitGroup

	chunkSize := len(ratings) / workerCount
	if chunkSize == 0 {
		chunkSize = len(ratings)
	}

	for i := 0; i < workerCount; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(ratings) {
			end = len(ratings)
		}
		if start >= len(ratings) {
			break
		}

		wg.Add(1)
		go func(chunk []Rating) {
			defer wg.Done()
			localCounts := make(map[float64]int)
			localUsers := make(map[string]bool)
			localMovies := make(map[string]bool)

			for _, r := range chunk {
				localCounts[r.Rating]++
				localUsers[r.UserID] = true
				localMovies[r.MovieID] = true
			}

			mu.Lock()
			for k, v := range localCounts {
				counts[k] += v
			}
			for u := range localUsers {
				userSet[u] = true
			}
			for m := range localMovies {
				movieSet[m] = true
			}
			mu.Unlock()
		}(ratings[start:end])
	}

	wg.Wait()

	saveDistribution(counts, "analisis/analysis_rating_distribution.csv")
	saveSummary(userSet, movieSet, len(ratings))
}

func saveDistribution(dist map[float64]int, filename string) {
	file, _ := os.Create(filename)
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Rating", "Count"})
	for rating, count := range dist {
		writer.Write([]string{fmt.Sprintf("%.1f", rating), strconv.Itoa(count)})
	}
}

func saveSummary(users, movies map[string]bool, total int) {
	file, _ := os.Create("analisis/analysis_summary.csv")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Metric", "Value"})
	writer.Write([]string{"Usuarios únicos", strconv.Itoa(len(users))})
	writer.Write([]string{"Películas únicas", strconv.Itoa(len(movies))})
	writer.Write([]string{"Total de ratings", strconv.Itoa(total)})
}

// ---------------------- TOP 10 PELÍCULAS ----------------------

func analyzeTopMovies(ratings []Rating, movies map[string]Movie) {
	counts := make(map[string]int)
	for _, r := range ratings {
		counts[r.MovieID]++
	}

	type pair struct {
		movieID string
		count   int
	}
	var pairs []pair
	for k, v := range counts {
		pairs = append(pairs, pair{k, v})
	}

	// Ordenamiento manual
	for i := 0; i < len(pairs)-1; i++ {
		for j := 0; j < len(pairs)-i-1; j++ {
			if pairs[j].count < pairs[j+1].count {
				pairs[j], pairs[j+1] = pairs[j+1], pairs[j]
			}
		}
	}

	file, _ := os.Create("analisis/analysis_top_movies.csv")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"MovieID", "Title", "RatingsCount"})
	for i := 0; i < 10 && i < len(pairs); i++ {
		m := movies[pairs[i].movieID]
		writer.Write([]string{m.MovieID, m.Title, strconv.Itoa(pairs[i].count)})
	}
}

// ---------------------- ANÁLISIS DE TAGS ----------------------

func analyzeTags(tags []Tag) {
	userCount := make(map[string]int)
	movieCount := make(map[string]int)

	for _, t := range tags {
		userCount[t.UserID]++
		movieCount[t.MovieID]++
	}

	file, _ := os.Create("analisis/analysis_tags_stats.csv")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"UserID", "TagCount"})
	for u, c := range userCount {
		writer.Write([]string{u, strconv.Itoa(c)})
	}

	writer.Write([]string{})
	writer.Write([]string{"MovieID", "TagCount"})
	for m, c := range movieCount {
		writer.Write([]string{m, strconv.Itoa(c)})
	}
}

// ---------------------- PROMEDIOS DE PELÍCULAS ----------------------

func analyzeMovieAverages(ratings []Rating, movies map[string]Movie) {
	type avgData struct {
		sum   float64
		count int
	}
	avg := make(map[string]*avgData)

	for _, r := range ratings {
		if _, ok := avg[r.MovieID]; !ok {
			avg[r.MovieID] = &avgData{}
		}
		avg[r.MovieID].sum += r.Rating
		avg[r.MovieID].count++
	}

	file, _ := os.Create("analisis/analysis_movie_avg.csv")
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"MovieID", "Title", "AvgRating", "Count"})

	type pair struct {
		id    string
		avg   float64
		count int
	}
	var pairs []pair
	for id, v := range avg {
		pairs = append(pairs, pair{id, v.sum / float64(v.count), v.count})
		writer.Write([]string{id, movies[id].Title, fmt.Sprintf("%.3f", v.sum/float64(v.count)), strconv.Itoa(v.count)})
	}

	// Ordenar para top 10
	for i := 0; i < len(pairs)-1; i++ {
		for j := 0; j < len(pairs)-i-1; j++ {
			if pairs[j].avg < pairs[j+1].avg {
				pairs[j], pairs[j+1] = pairs[j+1], pairs[j]
			}
		}
	}

	topFile, _ := os.Create("analisis/analysis_best_movies.csv")
	defer topFile.Close()
	topWriter := csv.NewWriter(topFile)
	defer topWriter.Flush()
	topWriter.Write([]string{"MovieID", "Title", "AvgRating", "Count"})
	for i := 0; i < 10 && i < len(pairs); i++ {
		m := movies[pairs[i].id]
		topWriter.Write([]string{m.MovieID, m.Title, fmt.Sprintf("%.3f", pairs[i].avg), strconv.Itoa(pairs[i].count)})
	}
}

// ---------------------- ANÁLISIS DE GÉNEROS ----------------------

func analyzeGenres(ratings []Rating, movies map[string]Movie) {
	type gData struct {
		sum   float64
		count int
	}

	genreMap := make(map[string]*gData)
	for _, r := range ratings {
		m, ok := movies[r.MovieID]
		if !ok {
			continue
		}
		genres := strings.Split(m.Genres, "|")
		for _, g := range genres {
			if g == "(no genres listed)" || g == "" {
				continue
			}
			if _, ok := genreMap[g]; !ok {
				genreMap[g] = &gData{}
			}
			genreMap[g].sum += r.Rating
			genreMap[g].count++
		}
	}

	// Guardar conteo
	countFile, _ := os.Create("analisis/analysis_genres_count.csv")
	defer countFile.Close()
	countWriter := csv.NewWriter(countFile)
	defer countWriter.Flush()
	countWriter.Write([]string{"Genre", "Reviews"})
	for g, v := range genreMap {
		countWriter.Write([]string{g, strconv.Itoa(v.count)})
	}

	// Guardar promedios
	avgFile, _ := os.Create("analisis/analysis_genres_avg.csv")
	defer avgFile.Close()
	avgWriter := csv.NewWriter(avgFile)
	defer avgWriter.Flush()
	avgWriter.Write([]string{"Genre", "AvgRating", "Reviews"})
	for g, v := range genreMap {
		avg := v.sum / float64(v.count)
		avgWriter.Write([]string{g, fmt.Sprintf("%.3f", avg), strconv.Itoa(v.count)})
	}
}

// ---------------------- MATRIZ USUARIO–PELÍCULA ----------------------

func generateUserMovieMatrix(ratings []Rating, maxUsers int) {
	file, _ := os.Create("analisis/matrix_user_movie.csv")
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"UserID", "MovieID", "Rating"})
	userCount := make(map[string]int)

	for _, r := range ratings {
		if userCount[r.UserID] >= 50 {
			continue
		}
		writer.Write([]string{r.UserID, r.MovieID, fmt.Sprintf("%.1f", r.Rating)})
		userCount[r.UserID]++
		if len(userCount) > maxUsers {
			break
		}
	}
}
