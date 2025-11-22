package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"pcd-pc4/internal/knn"
	"pcd-pc4/pkg/database"
	"pcd-pc4/pkg/network"
)

var (
	userRatings map[string]map[string]float64
	movieTitles map[string]string

	// Nombres de contenedor Docker
	nodes = []string{
		"pcd-pc4_nodo1:9000",
		"pcd-pc4_nodo2:9001",
	}
)

const (
	K    = 50
	TopN = 10
)

func main() {
	fmt.Println("Cargando datos limpios de MovieLens...")

	userRatings = knn.LoadUserRatings("data/clean/ratings.csv")
	movieTitles = knn.LoadMovieTitles("data/clean/movies.csv")

	if len(userRatings) == 0 {
		log.Fatal("No se pudieron cargar ratings.")
	}

	// --------------------------------------------------
	// Conexión a MongoDB
	// --------------------------------------------------

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://pcd-pc4_mongo:27017"
	}

	fmt.Println("Conectando a MongoDB en:", uri)

	if err := database.Connect(uri); err != nil {
		log.Fatal("Error conectando a MongoDB: ", err)
	}

	fmt.Println("Conexión a MongoDB lista.")

	// --------------------------------------------------
	// Iniciar servidor HTTP
	// --------------------------------------------------

	fmt.Println("API distribuida escuchando en puerto 8080...")

	http.HandleFunc("/recommend/", handleRecommendUser)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

// -----------------------------------------------------------
// ENDPOINT: GET /recommend/:userID
// -----------------------------------------------------------

func handleRecommendUser(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Path[len("/recommend/"):]
	if user == "" {
		http.Error(w, "Debe especificar un usuario", 400)
		return
	}

	if _, ok := userRatings[user]; !ok {
		http.Error(w, "Usuario no encontrado", 404)
		return
	}

	start := time.Now()

	recs, err := distributedRecommendation(user)
	if err != nil {
		http.Error(w, "Error en recomendación: "+err.Error(), 500)
		return
	}

	latency := time.Since(start).Milliseconds()

	// Guardar historial en MongoDB (asíncrono)
	go saveRecommendationToMongo(user, recs, latency)

	// Responder
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(recs)
}

// -----------------------------------------------------------
// PROCESO DISTRIBUIDO: API → nodos ML
// -----------------------------------------------------------

func distributedRecommendation(targetUser string) ([]knn.Recommended, error) {
	// Dividir usuarios en chunks (uno por nodo)
	chunks := splitUsersIntoChunks(userRatings, len(nodes))

	allNeighbors := []network.NeighborResult{}

	for i, chunk := range chunks {
		addr := nodes[i]

		partial, err := sendTaskToNode(addr, targetUser, chunk)
		if err != nil {
			return nil, err
		}

		allNeighbors = append(allNeighbors, partial...)
	}

	// Selección global de top K vecinos
	topK := knn.TopK(allNeighbors, K)

	// Predecir ratings
	recs := knn.PredictRatings(targetUser, userRatings, topK)

	return knn.TopNRecommendations(recs, TopN), nil
}

// -----------------------------------------------------------
// TCP: enviar tarea a cada nodo
// -----------------------------------------------------------

func sendTaskToNode(addr, target string, chunk map[string]map[string]float64) ([]network.NeighborResult, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Error conectando a nodo", addr, ":", err)
		return nil, err
	}
	defer conn.Close()

	req := network.TaskRequest{
		TargetUser: target,
		UserChunk:  chunk,
		K:          K,
	}

	if err := network.Send(conn, req); err != nil {
		return nil, err
	}

	var resp network.TaskResponse
	if err := network.Receive(conn, &resp); err != nil {
		return nil, err
	}

	return resp.PartialNeighbors, nil
}

// -----------------------------------------------------------
// GUARDAR RECOMENDACIÓN EN MONGODB
// -----------------------------------------------------------

func saveRecommendationToMongo(user string, recs []knn.Recommended, latencyMS int64) {
	col := database.RecsCollection()

	// Convertimos recs (knn.Recommended) → RecommendedItem
	items := make([]database.RecommendedItem, 0, len(recs))
	for _, r := range recs {
		items = append(items, database.RecommendedItem{
			MovieID:   r.MovieID,
			Predicted: r.Predicted,
		})
	}

	doc := database.RecommendationDocument{
		UserID:        user,
		Recommended:   items,
		LatencyMS:     latencyMS,
		TimestampUnix: time.Now().Unix(),
	}

	_, err := col.InsertOne(context.Background(), doc)
	if err != nil {
		fmt.Println("Error guardando recomendación:", err)
	}
}

// -----------------------------------------------------------
// Dividir usuarios en N partes
// -----------------------------------------------------------

func splitUsersIntoChunks(data map[string]map[string]float64, parts int) []map[string]map[string]float64 {
	chunks := make([]map[string]map[string]float64, parts)

	for i := 0; i < parts; i++ {
		chunks[i] = make(map[string]map[string]float64)
	}

	i := 0
	for user, ratings := range data {
		idx := i % parts
		chunks[idx][user] = ratings
		i++
	}

	return chunks
}
