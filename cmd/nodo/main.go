package main

import (
	"fmt"
	"net"
	"os"

	"pcd-pc4/internal/knn"
	"pcd-pc4/pkg/network"
)

func main() {
	// Leer puerto desde variable de entorno para soportar múltiples nodos
	port := os.Getenv("PORT")
	if port == "" {
		port = "9000" // valor por defecto
	}

	addr := ":" + port
	fmt.Println("Nodo ML escuchando en", addr)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	var req network.TaskRequest
	if err := network.Receive(conn, &req); err != nil {
		fmt.Println("Error recibiendo:", err)
		return
	}

	partial := computePartialNeighbors(req)

	resp := network.TaskResponse{
		PartialNeighbors: partial,
	}

	if err := network.Send(conn, resp); err != nil {
		fmt.Println("Error enviando respuesta:", err)
	}
}

func computePartialNeighbors(req network.TaskRequest) []network.NeighborResult {
	targetRatings := req.UserChunk[req.TargetUser]
	if targetRatings == nil {
		return nil
	}

	results := []network.NeighborResult{}

	for user, ratings := range req.UserChunk {
		if user == req.TargetUser {
			continue
		}

		sim := knn.CosineSimilarity(targetRatings, ratings)
		if sim > 0 {
			results = append(results, network.NeighborResult{
				UserID:     user,
				Similarity: sim,
			})
		}
	}

	return knn.TopK(results, req.K)
}
