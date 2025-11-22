package database

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// -------------------------
// Cliente Mongo Global
// -------------------------

var Client *mongo.Client

func Connect(uri string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	// Verificar conexión
	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	Client = client
	return nil
}

func RecsCollection() *mongo.Collection {
	return Client.Database("pcd").Collection("recommendations")
}

func LogsCollection() *mongo.Collection {
	return Client.Database("pcd").Collection("logs")
}
