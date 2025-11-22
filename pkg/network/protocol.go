package network

import (
	"encoding/gob"
	"net"
)

// -------------------- Tipos de Mensaje --------------------

type TaskRequest struct {
	TargetUser string                        // usuario al que queremos recomendar
	UserChunk  map[string]map[string]float64 // subset de usuarios asignados al nodo
	K          int                           // vecinos K
}

type TaskResponse struct {
	PartialNeighbors []NeighborResult // vecinos parciales
}

type NeighborResult struct {
	UserID     string
	Similarity float64
}

// -------------------- Utilidades --------------------

// Enviar mensaje genérico
func Send(conn net.Conn, v any) error {
	enc := gob.NewEncoder(conn)
	return enc.Encode(v)
}

// Recibir mensaje genérico
func Receive(conn net.Conn, v any) error {
	dec := gob.NewDecoder(conn)
	return dec.Decode(v)
}

func init() {
	// Registrar tipos para que gob pueda codificarlos
	gob.Register(TaskRequest{})
	gob.Register(TaskResponse{})
	gob.Register(NeighborResult{})
	gob.Register(map[string]map[string]float64{})
}
