package store

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	DB       *mongo.Database
	Posts    *mongo.Collection
	Users    *mongo.Collection
	Channels *mongo.Collection
}

func Connect(ctx context.Context, uri, dbName string) (*Mongo, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	m := &Mongo{
		DB:       db,
		Posts:    db.Collection("posts"),
		Users:    db.Collection("users"),
		Channels: db.Collection("channels"),
	}
	// Indexes
	_, _ = m.Posts.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "date", Value: -1}}},
		{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "msg_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	// TTL index (14 days)
	ttl := int32(14 * 24 * 3600)
	_, _ = m.Posts.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "date", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(ttl),
	})
	return m, nil
}

type PostDoc struct {
	ChannelID int64          `bson:"channel_id"`
	MsgID     int            `bson:"msg_id"`
	Date      time.Time      `bson:"date"`
	Text      string         `bson:"text,omitempty"`
	Views     int            `bson:"views,omitempty"`
	Forwards  int            `bson:"forwards,omitempty"`
	Reactions map[string]int `bson:"reactions,omitempty"`
}

func (m *Mongo) UpsertPost(ctx context.Context, p PostDoc) error {
	_, err := m.Posts.UpdateOne(ctx,
		bson.M{"channel_id": p.ChannelID, "msg_id": p.MsgID},
		bson.M{"$setOnInsert": p},
		options.Update().SetUpsert(true),
	)
	return err
}
