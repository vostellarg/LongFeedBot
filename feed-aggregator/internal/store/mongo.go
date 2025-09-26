package store

import (
	"context"
	"strings"
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

// ---------- User / Channel ----------

type UserDoc struct {
	ID            int64     `bson:"_id"`
	Subscriptions []string  `bson:"subscriptions,omitempty"` // canonical lowercase usernames w/o '@'
	CreatedAt     time.Time `bson:"created_at,omitempty"`
	UpdatedAt     time.Time `bson:"updated_at,omitempty"`
}

type ChannelDoc struct {
	// We now key the document by a canonical "key" string:
	//   public: "u:<username>"  (username lowercased, no '@')
	//   invite: "i:<hash>"
	Username       string     `bson:"username,omitempty"` // if known/resolved
	Key            string     `bson:"_id"`                // "u:foo" or "i:AbCdEf..."
	ChannelID      int64      `bson:"channel_id,omitempty"`
	AccessHash     int64      `bson:"access_hash,omitempty"`
	Title          string     `bson:"title,omitempty"`
	Subscribers    int64      `bson:"subscribers,omitempty"`
	Aliases        []string   `bson:"aliases,omitempty"` // any equivalent inputs (e.g., multiple invite links)
	LastResolvedAt *time.Time `bson:"last_resolved_at,omitempty"`
	LastJoinedAt   *time.Time `bson:"last_joined_at,omitempty"`
	LastLeftAt     *time.Time `bson:"last_left_at,omitempty"`
}

// ---------- Rich Post Model ----------

type MessageEntity struct {
	Type     string `bson:"type"`
	Offset   int    `bson:"offset,omitempty"`
	Length   int    `bson:"length,omitempty"`
	URL      string `bson:"url,omitempty"`
	Language string `bson:"language,omitempty"`
	UserID   int64  `bson:"user_id,omitempty"`
	EmojiID  int64  `bson:"emoji_id,omitempty"`
}

type FileRef struct {
	ID            int64  `bson:"id"`
	AccessHash    int64  `bson:"access_hash"`
	FileReference []byte `bson:"file_reference,omitempty"`
	DC            int    `bson:"dc_id,omitempty"`
	FileName      string `bson:"file_name,omitempty"`
	MimeType      string `bson:"mime_type,omitempty"`
	Size          int64  `bson:"size,omitempty"`
}

// A small preview. We keep dimensions and (if present) tiny inlined bytes.
// Bytes come from PhotoCachedSize / PhotoStrippedSize and are small.
type PhotoSize struct {
	W     int      `bson:"w"`
	H     int      `bson:"h"`
	Type  string   `bson:"type"`            // s,m,x,y,w,... or "stripped"
	Bytes []byte   `bson:"bytes,omitempty"` // tiny preview when available
	File  *FileRef `bson:"file,omitempty"`  // rarely available directly on sizes
}

type MediaPhoto struct {
	Sizes       []PhotoSize `bson:"sizes,omitempty"` // pick a best-fit in frontend
	HasStickers bool        `bson:"has_stickers,omitempty"`
}

type MediaVideo struct {
	File     FileRef     `bson:"file"`
	Duration int         `bson:"duration,omitempty"`
	W        int         `bson:"w,omitempty"`
	H        int         `bson:"h,omitempty"`
	Round    bool        `bson:"round,omitempty"`
	Thumbs   []PhotoSize `bson:"thumbs,omitempty"` // includes “first frame” thumbs
}

type MediaAnimation struct { // GIF / animated webm etc. (DocumentAttributeAnimated)
	File   FileRef     `bson:"file"`
	Thumbs []PhotoSize `bson:"thumbs,omitempty"`
}

type MediaSticker struct {
	File     FileRef     `bson:"file"`
	Alt      string      `bson:"alt,omitempty"`
	Thumbs   []PhotoSize `bson:"thumbs,omitempty"`
	Animated bool        `bson:"animated,omitempty"`
	Video    bool        `bson:"video,omitempty"`
}

type MediaAudio struct {
	File      FileRef `bson:"file"`
	Duration  int     `bson:"duration,omitempty"`
	Title     string  `bson:"title,omitempty"`
	Performer string  `bson:"performer,omitempty"`
}

type MediaVoice struct {
	File     FileRef `bson:"file"`
	Duration int     `bson:"duration,omitempty"`
	Waveform []byte  `bson:"waveform,omitempty"`
}

type WebPagePreview struct {
	URL         string      `bson:"url,omitempty"`
	DisplayURL  string      `bson:"display_url,omitempty"`
	SiteName    string      `bson:"site_name,omitempty"`
	Title       string      `bson:"title,omitempty"`
	Description string      `bson:"description,omitempty"`
	Photo       *MediaPhoto `bson:"photo,omitempty"`
}

type MediaPoll struct {
	ID             int64    `bson:"id"`
	Question       string   `bson:"question"`
	Closed         bool     `bson:"closed,omitempty"`
	PublicVoters   bool     `bson:"public_voters,omitempty"`
	MultipleChoice bool     `bson:"multiple_choice,omitempty"`
	Quiz           bool     `bson:"quiz,omitempty"`
	Options        []string `bson:"options,omitempty"` // option text; counts only if available
	// OptionCounts   []int  `bson:"option_counts,omitempty"` // add later if needed
}

type MediaGeo struct {
	Lat            float64 `bson:"lat"`
	Lon            float64 `bson:"lon"`
	AccuracyRadius int     `bson:"accuracy_radius,omitempty"`
}

type Media struct {
	Kind      string          `bson:"kind"` // photo, video, animation, sticker, audio, voice, poll, geo, webpage, document, none
	Photo     *MediaPhoto     `bson:"photo,omitempty"`
	Video     *MediaVideo     `bson:"video,omitempty"`
	Animation *MediaAnimation `bson:"animation,omitempty"`
	Sticker   *MediaSticker   `bson:"sticker,omitempty"`
	Audio     *MediaAudio     `bson:"audio,omitempty"`
	Voice     *MediaVoice     `bson:"voice,omitempty"`
	WebPage   *WebPagePreview `bson:"webpage,omitempty"`
	Poll      *MediaPoll      `bson:"poll,omitempty"`
	Geo       *MediaGeo       `bson:"geo,omitempty"`
	// You can add Document generic if you want to expose raw docs
}

type ForwardInfo struct {
	FromName      string `bson:"from_name,omitempty"`
	FromUserID    int64  `bson:"from_user_id,omitempty"`
	FromChannelID int64  `bson:"from_channel_id,omitempty"`
	PostAuthor    string `bson:"post_author,omitempty"`
	FromMessageID int    `bson:"from_message_id,omitempty"`
	Date          int64  `bson:"date,omitempty"`
}

type PostDoc struct {
	ChannelID int64      `bson:"channel_id"`
	MsgID     int        `bson:"msg_id"`
	Date      time.Time  `bson:"date"`
	EditDate  *time.Time `bson:"edit_date,omitempty"`

	// Content
	Text      string          `bson:"text,omitempty"`
	Entities  []MessageEntity `bson:"entities,omitempty"`
	Media     *Media          `bson:"media,omitempty"`
	GroupedID *int64          `bson:"grouped_id,omitempty"` // albums

	// Relations
	ReplyToMsgID *int         `bson:"reply_to_msg_id,omitempty"`
	FwdFrom      *ForwardInfo `bson:"fwd_from,omitempty"`
	ViaBotID     *int64       `bson:"via_bot_id,omitempty"`
	Signature    string       `bson:"signature,omitempty"`

	// Stats
	Views     int            `bson:"views,omitempty"`
	Forwards  int            `bson:"forwards,omitempty"`
	Reactions map[string]int `bson:"reactions,omitempty"`

	InsertedAt time.Time `bson:"inserted_at,omitempty"`
}

// For refresh scheduling
type RefreshKey struct {
	ChannelID int64     `bson:"channel_id"`
	MsgID     int       `bson:"msg_id"`
	Date      time.Time `bson:"date"`
}

// ---------- Connect & Indexes ----------

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
	_, _ = m.Posts.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "date", Value: -1}}},
		{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "msg_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	ttl := int32(14 * 24 * 3600)
	_, _ = m.Posts.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "date", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(ttl),
	})
	_, _ = m.Users.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "subscriptions", Value: 1}}})
	_, _ = m.Channels.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "channel_id", Value: 1}}},
	})
	return m, nil
}

// ---------- Helpers & Upserts ----------

func CanonicalUsername(u string) string {
	u = strings.TrimSpace(u)
	if u == "" {
		return u
	}
	if u[0] == '@' {
		u = u[1:]
	}
	return strings.ToLower(u)
}

func (m *Mongo) UpsertPost(ctx context.Context, p PostDoc) error {
	if p.InsertedAt.IsZero() {
		p.InsertedAt = time.Now().UTC()
	}
	_, err := m.Posts.UpdateOne(
		ctx,
		bson.M{"channel_id": p.ChannelID, "msg_id": p.MsgID},
		bson.M{"$setOnInsert": p},
		options.Update().SetUpsert(true),
	)
	return err
}

func (m *Mongo) UpdatePostMetrics(ctx context.Context, channelID int64, msgID int, views, forwards *int, reactions map[string]int, editDate *time.Time) error {
	set := bson.M{}
	if views != nil {
		set["views"] = *views
	}
	if forwards != nil {
		set["forwards"] = *forwards
	}
	if reactions != nil {
		set["reactions"] = reactions
	}
	if editDate != nil {
		set["edit_date"] = *editDate
	}
	if len(set) == 0 {
		return nil
	}
	_, err := m.Posts.UpdateOne(
		ctx,
		bson.M{"channel_id": channelID, "msg_id": msgID},
		bson.M{"$set": set},
	)
	return err
}

// User & Channel helpers (keep existing implementations in your file):
// - GetAllSubscribedUsernames
// - UpsertUserSubscriptions
// - UpsertChannelResolution
// - MarkChannelJoined

// ---------- User & Channel operations ----------

// GetAllSubscribedUsernames returns the union of all usernames present in users.subscriptions.
func (m *Mongo) GetAllSubscribedUsernames(ctx context.Context) ([]string, error) {
	cur, err := m.Users.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"subscriptions": 1}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	seen := make(map[string]struct{}, 1024)
	for cur.Next(ctx) {
		var doc struct {
			Subscriptions []string `bson:"subscriptions"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		for _, s := range doc.Subscriptions {
			cs := CanonicalUsername(s)
			if cs != "" {
				seen[cs] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out, cur.Err()
}

// UpsertUserSubscriptions sets or replaces a user's subscriptions with canonicalized usernames.
func (m *Mongo) UpsertUserSubscriptions(ctx context.Context, userID int64, subs []string) error {
	clean := make([]string, 0, len(subs))
	for _, s := range subs {
		cs := CanonicalUsername(s)
		if cs != "" {
			clean = append(clean, cs)
		}
	}
	now := time.Now().UTC()
	_, err := m.Users.UpdateByID(ctx, userID, bson.M{
		"$set": bson.M{
			"subscriptions": clean,
			"updated_at":    now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}, options.Update().SetUpsert(true))
	return err
}

// UpsertChannelResolution writes/updates resolved channel info for a username.
func (m *Mongo) UpsertChannelResolution(ctx context.Context, username string, channelID, accessHash int64, title string, subscribers int64) error {
	username = CanonicalUsername(username)
	now := time.Now().UTC()
	_, err := m.Channels.UpdateByID(ctx, username, bson.M{
		"$set": bson.M{
			"channel_id":       channelID,
			"access_hash":      accessHash,
			"title":            title,
			"subscribers":      subscribers,
			"last_resolved_at": now,
		},
		"$setOnInsert": bson.M{
			"_id": username,
		},
	}, options.Update().SetUpsert(true))
	return err
}

// MarkChannelJoined stamps last_joined_at for the username.
func (m *Mongo) MarkChannelJoined(ctx context.Context, username string) error {
	username = CanonicalUsername(username)
	now := time.Now().UTC()
	_, err := m.Channels.UpdateByID(ctx, username, bson.M{
		"$set": bson.M{"last_joined_at": now},
		"$setOnInsert": bson.M{
			"_id": username,
		},
	}, options.Update().SetUpsert(true))
	return err
}

// GetUserSubscriptions returns canonicalized usernames for a user.
func (m *Mongo) GetUserSubscriptions(ctx context.Context, userID int64) ([]string, error) {
	var doc struct {
		Subscriptions []string `bson:"subscriptions"`
	}
	err := m.Users.FindOne(ctx, bson.M{"_id": userID}, options.FindOne().SetProjection(bson.M{"subscriptions": 1})).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(doc.Subscriptions))
	for _, s := range doc.Subscriptions {
		if cs := CanonicalUsername(s); cs != "" {
			out = append(out, cs)
		}
	}
	return out, nil
}

// GetChannelsByUsernames returns channel docs for given usernames (found set only).
func (m *Mongo) GetChannelsByUsernames(ctx context.Context, usernames []string) (map[string]ChannelDoc, error) {
	if len(usernames) == 0 {
		return map[string]ChannelDoc{}, nil
	}
	cur, err := m.Channels.Find(ctx, bson.M{"_id": bson.M{"$in": usernames}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	res := make(map[string]ChannelDoc, len(usernames))
	for cur.Next(ctx) {
		var cd ChannelDoc
		if err := cur.Decode(&cd); err != nil {
			return nil, err
		}
		res[cd.Username] = cd
	}
	return res, cur.Err()
}

// GetChannelsByIDs maps channel_id -> ChannelDoc (for feed enrichment).
func (m *Mongo) GetChannelsByIDs(ctx context.Context, ids []int64) (map[int64]ChannelDoc, error) {
	if len(ids) == 0 {
		return map[int64]ChannelDoc{}, nil
	}
	cur, err := m.Channels.Find(ctx, bson.M{"channel_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	res := make(map[int64]ChannelDoc, len(ids))
	for cur.Next(ctx) {
		var cd ChannelDoc
		if err := cur.Decode(&cd); err != nil {
			return nil, err
		}
		res[cd.ChannelID] = cd
	}
	return res, cur.Err()
}

// FindRecentPostsByChannels returns recent posts, newest first, limited.
func (m *Mongo) FindRecentPostsByChannels(ctx context.Context, channelIDs []int64, since time.Time, limit int) ([]PostDoc, error) {
	if len(channelIDs) == 0 {
		return []PostDoc{}, nil
	}
	f := bson.M{
		"channel_id": bson.M{"$in": channelIDs},
		"date":       bson.M{"$gte": since},
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "date", Value: -1}}).
		SetLimit(int64(limit))
	cur, err := m.Posts.Find(ctx, f, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make([]PostDoc, 0, limit)
	for cur.Next(ctx) {
		var p PostDoc
		if err := cur.Decode(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, cur.Err()
}

// MarkChannelLeft records a left timestamp (for observability / cleanup).
func (m *Mongo) MarkChannelLeft(ctx context.Context, username string) error {
	username = CanonicalUsername(username)
	now := time.Now().UTC()
	_, err := m.Channels.UpdateByID(ctx, username, bson.M{
		"$set": bson.M{"last_left_at": now},
		"$setOnInsert": bson.M{
			"_id": username,
		},
	}, options.Update().SetUpsert(true))
	return err
}

// CanonicalKey returns a storage key from input kind+value.
func CanonicalKey(kind, val string) string {
	val = CanonicalUsername(val) // lowercases; safe for hash too
	if kind == "username" {
		return "u:" + val
	}
	if kind == "invite" {
		return "i:" + val
	}
	return val
}

// UpsertChannelResolutionByKey writes/updates channel info for given key ("u:..." or "i:...").
func (m *Mongo) UpsertChannelResolutionByKey(ctx context.Context, key, username string, channelID, accessHash int64, title string, subscribers int64, aliases []string) error {
	now := time.Now().UTC()
	update := bson.M{
		"$set": bson.M{
			"username":         CanonicalUsername(username),
			"channel_id":       channelID,
			"access_hash":      accessHash,
			"title":            title,
			"subscribers":      subscribers,
			"last_resolved_at": now,
		},
		"$setOnInsert": bson.M{
			"_id": key,
			"key": key,
		},
	}
	if len(aliases) > 0 {
		update["$addToSet"] = bson.M{"aliases": bson.M{"$each": aliases}}
	}
	_, err := m.Channels.UpdateByID(ctx, key, update, options.Update().SetUpsert(true))
	return err
}

func (m *Mongo) MarkChannelJoinedByKey(ctx context.Context, key string) error {
	now := time.Now().UTC()
	_, err := m.Channels.UpdateByID(ctx, key, bson.M{
		"$set":         bson.M{"last_joined_at": now},
		"$setOnInsert": bson.M{"_id": key, "key": key},
	}, options.Update().SetUpsert(true))
	return err
}

func (m *Mongo) MarkChannelLeftByKey(ctx context.Context, key string) error {
	now := time.Now().UTC()
	_, err := m.Channels.UpdateByID(ctx, key, bson.M{
		"$set":         bson.M{"last_left_at": now},
		"$setOnInsert": bson.M{"_id": key, "key": key},
	}, options.Update().SetUpsert(true))
	return err
}

// GetChannelsByInputs maps *raw user inputs* (links/handles) to ChannelDoc by using our canonical key.
func (m *Mongo) GetChannelsByInputs(ctx context.Context, inputs []string) (map[string]ChannelDoc, error) {
	if len(inputs) == 0 {
		return map[string]ChannelDoc{}, nil
	}
	// We'll canonicalize keys in the HTTP layer or reconciler (so here just fetch by _id $in).
	cur, err := m.Channels.Find(ctx, bson.M{"_id": bson.M{"$in": inputs}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	res := make(map[string]ChannelDoc, len(inputs))
	for cur.Next(ctx) {
		var cd ChannelDoc
		if err := cur.Decode(&cd); err != nil {
			return nil, err
		}
		res[cd.Key] = cd
	}
	return res, cur.Err()
}

// FindPostsForRefresh returns most-recent posts in [from, to) for given channels.
// It returns at most 'limit' documents, sorted by date desc.
func (m *Mongo) FindPostsForRefresh(ctx context.Context, channelIDs []int64, from, to time.Time, limit int) ([]RefreshKey, error) {
	if len(channelIDs) == 0 || limit <= 0 {
		return []RefreshKey{}, nil
	}
	f := bson.M{
		"channel_id": bson.M{"$in": channelIDs},
		"date":       bson.M{"$gte": from, "$lt": to},
	}
	opts := options.Find().
		SetProjection(bson.M{"channel_id": 1, "msg_id": 1, "date": 1, "_id": 0}).
		SetSort(bson.D{{Key: "date", Value: -1}}).
		SetLimit(int64(limit))
	cur, err := m.Posts.Find(ctx, f, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make([]RefreshKey, 0, limit)
	for cur.Next(ctx) {
		var k RefreshKey
		if err := cur.Decode(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, cur.Err()
}
