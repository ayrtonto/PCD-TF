package database

// -----------------------------------------------------------
// DOCUMENTO: Recomendación generada para un usuario
// Colección: recommendations
// -----------------------------------------------------------

type RecommendedItem struct {
	MovieID   string  `bson:"movie_id" json:"movie_id"`
	Predicted float64 `bson:"predicted" json:"predicted"`
}

type RecommendationDocument struct {
	UserID        string            `bson:"user_id" json:"user_id"`
	Recommended   []RecommendedItem `bson:"recommended" json:"recommended"`
	LatencyMS     int64             `bson:"latency_ms" json:"latency_ms"`
	TimestampUnix int64             `bson:"timestamp" json:"timestamp"`
}

// -----------------------------------------------------------
// DOCUMENTO: Log del proceso distribuido
// Colección: logs
// -----------------------------------------------------------

type LogDocument struct {
	UserID        string `bson:"user_id" json:"user_id"`
	NodeCount     int    `bson:"node_count" json:"node_count"`
	LatencyMS     int64  `bson:"latency_ms" json:"latency_ms"`
	TimestampUnix int64  `bson:"timestamp" json:"timestamp"`
}
