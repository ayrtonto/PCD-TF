package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"sync"
)

const workerCount = 8 // número de workers concurrentes

type Rating struct {
	UserID    string
	MovieID   string
	Rating    string
	Timestamp string
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
	fmt.Println("Iniciando limpieza concurrente de MovieLens...")

	cleanRatingsConcurrent("data/raw/ratings.dat", "data/clean/ratings.csv")
	cleanMoviesConcurrent("data/raw/movies.dat", "data/clean/movies.csv")
	cleanTagsConcurrent("data/raw/tags.dat", "data/clean/tags.csv")

	fmt.Println("Limpieza completa. Se generaron clean_ratings.csv, clean_movies.csv y clean_tags.csv")
}

// -------------------- Limpieza concurrente de RATINGS --------------------

func cleanRatingsConcurrent(inputPath, outputPath string) {
	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Println("Error al abrir", inputPath, ":", err)
		return
	}
	defer file.Close()

	lines := make(chan string, 10000)
	results := make(chan Rating, 10000)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				parts := strings.Split(line, "::")
				if len(parts) != 4 {
					continue
				}
				user, movie, rating, timestamp := parts[0], parts[1], parts[2], parts[3]
				if user == "" || movie == "" || rating == "" || timestamp == "" {
					continue
				}
				results <- Rating{user, movie, rating, timestamp}
			}
		}()
	}

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]bool)
	var cleanData [][]string
	for r := range results {
		key := r.UserID + "_" + r.MovieID + "_" + r.Timestamp
		if seen[key] {
			continue
		}
		seen[key] = true
		cleanData = append(cleanData, []string{r.UserID, r.MovieID, r.Rating, r.Timestamp})
	}

	saveToCSV(outputPath, []string{"UserID", "MovieID", "Rating", "Timestamp"}, cleanData)
	fmt.Println("Limpieza de ratings completada")
}

// -------------------- Limpieza concurrente de MOVIES --------------------

func cleanMoviesConcurrent(inputPath, outputPath string) {
	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Println("Error al abrir", inputPath, ":", err)
		return
	}
	defer file.Close()

	lines := make(chan string, 10000)
	results := make(chan Movie, 10000)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				parts := strings.Split(line, "::")
				if len(parts) != 3 {
					continue
				}
				id, title, genres := parts[0], parts[1], parts[2]
				if id == "" || title == "" {
					continue
				}
				results <- Movie{id, title, genres}
			}
		}()
	}

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]bool)
	var cleanData [][]string
	for m := range results {
		if seen[m.MovieID] {
			continue
		}
		seen[m.MovieID] = true
		cleanData = append(cleanData, []string{m.MovieID, m.Title, m.Genres})
	}

	saveToCSV(outputPath, []string{"MovieID", "Title", "Genres"}, cleanData)
	fmt.Println("Limpieza de movies completada")
}

// -------------------- Limpieza concurrente de TAGS --------------------

func cleanTagsConcurrent(inputPath, outputPath string) {
	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Println("Error al abrir", inputPath, ":", err)
		return
	}
	defer file.Close()

	lines := make(chan string, 10000)
	results := make(chan Tag, 10000)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				parts := strings.Split(line, "::")
				if len(parts) != 4 {
					continue
				}
				user, movie, tag, timestamp := parts[0], parts[1], parts[2], parts[3]
				if user == "" || movie == "" || tag == "" {
					continue
				}
				results <- Tag{user, movie, tag, timestamp}
			}
		}()
	}

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]bool)
	var cleanData [][]string
	for t := range results {
		key := t.UserID + "_" + t.MovieID + "_" + t.Tag + "_" + t.Timestamp
		if seen[key] {
			continue
		}
		seen[key] = true
		cleanData = append(cleanData, []string{t.UserID, t.MovieID, t.Tag, t.Timestamp})
	}

	saveToCSV(outputPath, []string{"UserID", "MovieID", "Tag", "Timestamp"}, cleanData)
	fmt.Println("Limpieza de tags completada")
}

// -------------------- Utilidad: exportar a CSV --------------------

func saveToCSV(path string, header []string, data [][]string) {
	file, err := os.Create(path)
	if err != nil {
		fmt.Println("Error al crear archivo CSV:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write(header)
	for _, record := range data {
		writer.Write(record)
	}
}
