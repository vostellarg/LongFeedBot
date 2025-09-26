package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"example.com/feed-aggregator/internal/ingest"
	"example.com/feed-aggregator/internal/store"

	"github.com/joho/godotenv"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
)

// ------------------ minimal stdin authenticator ------------------

type stdinAuth struct {
	in          *bufio.Reader
	out         *log.Logger
	phoneEnv    string
	codeEnv     string
	passwordEnv string
}

func newStdinAuth() *stdinAuth {
	return &stdinAuth{
		in:          bufio.NewReader(os.Stdin),
		out:         log.New(os.Stdout, "", 0),
		phoneEnv:    os.Getenv("TG_PHONE"),
		codeEnv:     os.Getenv("TG_CODE"),
		passwordEnv: os.Getenv("TG_PASSWORD"),
	}
}
func (a *stdinAuth) prompt(prompt, fallback string) (string, error) {
	if fallback != "" {
		a.out.Printf("%s (using env)\n", prompt)
		return fallback, nil
	}
	a.out.Print(prompt)
	line, err := a.in.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}
func (a *stdinAuth) Phone(ctx context.Context) (string, error) {
	return a.prompt("Enter phone (with +): ", a.phoneEnv)
}
func (a *stdinAuth) Code(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
	return a.prompt("Enter code: ", a.codeEnv)
}
func (a *stdinAuth) Password(ctx context.Context) (string, error) {
	return a.prompt("Enter 2FA password: ", a.passwordEnv)
}
func (a *stdinAuth) AcceptTermsOfService(ctx context.Context, tos tg.HelpTermsOfService) error {
	a.out.Printf("Please accept ToS to continue: %s\n", tos.Text)
	line, err := a.prompt("Type 'yes' to accept: ", "")
	if err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(line)) != "yes" {
		return fmt.Errorf("terms of service not accepted")
	}
	return nil
}

// SignUp implements the auth.UserAuthenticator interface for new users.
func (a *stdinAuth) SignUp(ctx context.Context) (auth.UserInfo, error) {
	name, err := a.prompt("Enter your first and last name (for sign up): ", "")
	if err != nil {
		return auth.UserInfo{}, err
	}
	parts := strings.Fields(name)
	if len(parts) == 0 {
		return auth.UserInfo{}, fmt.Errorf("no name entered")
	}
	first := parts[0]
	last := ""
	if len(parts) > 1 {
		last = strings.Join(parts[1:], " ")
	}
	return auth.UserInfo{FirstName: first, LastName: last}, nil
}

// ------------------ ID allowlist for tracked channels ------------------

type IDAllowlist struct {
	mu  sync.RWMutex
	ids map[int64]struct{}
}

func NewIDAllowlist() *IDAllowlist { return &IDAllowlist{ids: map[int64]struct{}{}} }
func (l *IDAllowlist) SetAll(in []int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ids = make(map[int64]struct{}, len(in))
	for _, id := range in {
		l.ids[id] = struct{}{}
	}
}
func (l *IDAllowlist) Add(id int64) {
	l.mu.Lock()
	l.ids[id] = struct{}{}
	l.mu.Unlock()
}
func (l *IDAllowlist) Remove(id int64) {
	l.mu.Lock()
	delete(l.ids, id)
	l.mu.Unlock()
}
func (l *IDAllowlist) Has(id int64) bool {
	l.mu.RLock()
	_, ok := l.ids[id]
	l.mu.RUnlock()
	return ok
}
func (l *IDAllowlist) Snapshot() []int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]int64, 0, len(l.ids))
	for id := range l.ids {
		out = append(out, id)
	}
	return out
}

// ------------------ parsing links/handles → keys ------------------

type parsedInput struct {
	Kind       string // "username" | "invite"
	Key        string // "u:<username>" | "i:<hash>"
	Username   string // if Kind=username
	InviteHash string // if Kind=invite
}

func parseChannelInput(s string) (*parsedInput, error) {
	orig := strings.TrimSpace(s)
	if orig == "" {
		return nil, fmt.Errorf("empty input")
	}
	if strings.HasPrefix(orig, "@") && len(orig) > 1 {
		u := store.CanonicalUsername(orig[1:])
		return &parsedInput{Kind: "username", Key: store.CanonicalKey("username", u), Username: u}, nil
	}
	if !strings.HasPrefix(orig, "http://") && !strings.HasPrefix(orig, "https://") {
		u := store.CanonicalUsername(orig)
		return &parsedInput{Kind: "username", Key: store.CanonicalKey("username", u), Username: u}, nil
	}
	u, err := url.Parse(orig)
	if err != nil {
		return nil, fmt.Errorf("bad url")
	}
	host := strings.ToLower(u.Host)
	if host == "t.me" || host == "telegram.me" || host == "www.t.me" || host == "www.telegram.me" {
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			return nil, fmt.Errorf("unsupported t.me url")
		}
		if parts[0] == "c" {
			return nil, fmt.Errorf("t.me/c links are not joinable; use public @username or invite link")
		}
		if parts[0] == "s" && len(parts) >= 2 {
			username := store.CanonicalUsername(parts[1])
			return &parsedInput{Kind: "username", Key: store.CanonicalKey("username", username), Username: username}, nil
		}
		if strings.HasPrefix(parts[0], "+") {
			hash := parts[0][1:]
			return &parsedInput{Kind: "invite", Key: store.CanonicalKey("invite", hash), InviteHash: hash}, nil
		}
		if parts[0] == "joinchat" && len(parts) >= 2 {
			hash := parts[1]
			return &parsedInput{Kind: "invite", Key: store.CanonicalKey("invite", hash), InviteHash: hash}, nil
		}
		username := store.CanonicalUsername(parts[0])
		return &parsedInput{Kind: "username", Key: store.CanonicalKey("username", username), Username: username}, nil
	}
	return nil, fmt.Errorf("not a telegram link/handle")
}

// ------------------ join/leave ------------------

func ensureJoinByUsername(ctx context.Context, raw *tg.Client, username string) (*tg.Channel, error) {
	uname := store.CanonicalUsername(username)
	if uname == "" {
		return nil, fmt.Errorf("empty username")
	}
	res, err := raw.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: uname})
	if err != nil {
		return nil, fmt.Errorf("resolve '%s': %w", uname, err)
	}
	var ch *tg.Channel
	for _, chat := range res.Chats {
		if c, ok := chat.(*tg.Channel); ok {
			ch = c
			break
		}
	}
	if ch == nil {
		return nil, fmt.Errorf("'%s' did not resolve to a channel/supergroup", uname)
	}
	input := &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}
	_, err = raw.ChannelsJoinChannel(ctx, input)
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "USER_ALREADY_PARTICIPANT") {
		return ch, err
	}
	return ch, nil
}

func ensureJoinByInvite(ctx context.Context, raw *tg.Client, hash string) (*tg.Channel, error) {
	upds, err := raw.MessagesImportChatInvite(ctx, hash)
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "USER_ALREADY_PARTICIPANT") {
		return nil, fmt.Errorf("import invite failed: %w", err)
	}
	var ch *tg.Channel
	switch u := upds.(type) {
	case *tg.Updates:
		for _, chat := range u.Chats {
			if c, ok := chat.(*tg.Channel); ok {
				ch = c
				break
			}
		}
	case *tg.UpdatesCombined:
		for _, chat := range u.Chats {
			if c, ok := chat.(*tg.Channel); ok {
				ch = c
				break
			}
		}
	}
	if ch == nil {
		if inv, err := raw.MessagesCheckChatInvite(ctx, hash); err == nil {
			if a, ok := inv.(*tg.ChatInviteAlready); ok {
				if c, ok := a.Chat.(*tg.Channel); ok {
					ch = c
				}
			}
		}
	}
	if ch == nil {
		return nil, fmt.Errorf("could not locate channel after importing invite")
	}
	return ch, nil
}

func leaveByKey(ctx context.Context, raw *tg.Client, ch store.ChannelDoc) error {
	if ch.ChannelID != 0 && ch.AccessHash != 0 {
		input := &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}
		_, err := raw.ChannelsLeaveChannel(ctx, input)
		return err
	}
	if ch.Username != "" {
		_, err := ensureJoinByUsername(ctx, raw, ch.Username)
		return err
	}
	return nil
}

// ------------------ reconciler ------------------

type reconciler struct {
	DB        *store.Mongo
	Raw       *tg.Client
	Logger    *log.Logger
	Allowlist *IDAllowlist

	mu           sync.Mutex
	currentUnion map[string]*parsedInput // keys: "u:..." or "i:..."
}

func newReconciler(db *store.Mongo, raw *tg.Client, logger *log.Logger, allow *IDAllowlist) *reconciler {
	return &reconciler{
		DB:           db,
		Raw:          raw,
		Logger:       logger,
		Allowlist:    allow,
		currentUnion: map[string]*parsedInput{},
	}
}

func (r *reconciler) run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	r.reconcileOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.reconcileOnce(ctx)
		}
	}
}

func (r *reconciler) reconcileOnce(ctx context.Context) {
	rawInputs, err := r.DB.GetAllSubscribedUsernames(ctx)
	if err != nil {
		r.Logger.Printf("reconcile: GetAllSubscribedUsernames error: %v", err)
		return
	}
	desired := make(map[string]*parsedInput, len(rawInputs))
	for _, in := range rawInputs {
		pi, err := parseChannelInput(in)
		if err != nil {
			r.Logger.Printf("skip input '%s': %v", in, err)
			continue
		}
		desired[pi.Key] = pi
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Determine toJoin and toLeave
	var toJoin []*parsedInput
	for k, pi := range desired {
		if _, ok := r.currentUnion[k]; !ok {
			toJoin = append(toJoin, pi)
		}
	}
	var toLeave []string
	for k := range r.currentUnion {
		if _, ok := desired[k]; !ok {
			toLeave = append(toLeave, k)
		}
	}

	// Join
	for _, pi := range toJoin {
		var ch *tg.Channel
		var jerr error
		switch pi.Kind {
		case "username":
			ch, jerr = ensureJoinByUsername(ctx, r.Raw, pi.Username)
		case "invite":
			ch, jerr = ensureJoinByInvite(ctx, r.Raw, pi.InviteHash)
		}
		if jerr != nil {
			r.Logger.Printf("join '%s' error: %v", pi.Key, jerr)
		} else if ch != nil {
			aliases := []string{pi.Key}
			_ = r.DB.UpsertChannelResolutionByKey(ctx, pi.Key, ch.Username, ch.ID, ch.AccessHash, ch.Title, 0, aliases)
			_ = r.DB.MarkChannelJoinedByKey(ctx, pi.Key)
			r.Allowlist.Add(ch.ID) // <- start accepting posts
			r.Logger.Printf("joined %s (id=%d)", pi.Key, ch.ID)
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}

	// Leave
	if len(toLeave) > 0 {
		meta, _ := r.DB.GetChannelsByInputs(ctx, toLeave)
		for _, key := range toLeave {
			if chdoc, ok := meta[key]; ok {
				if err := leaveByKey(ctx, r.Raw, chdoc); err != nil {
					r.Logger.Printf("leave '%s' error: %v", key, err)
				} else {
					_ = r.DB.MarkChannelLeftByKey(ctx, key)
					if chdoc.ChannelID != 0 {
						r.Allowlist.Remove(chdoc.ChannelID)
					}
					r.Logger.Printf("left %s", key)
				}
				select {
				case <-time.After(500 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}
	}

	// Snapshot & rebuild allowlist from desired keys -> channelIDs
	keys := make([]string, 0, len(desired))
	for k := range desired {
		keys = append(keys, k)
	}
	chDocs, _ := r.DB.GetChannelsByInputs(ctx, keys)

	var activeIDs []int64
	for _, cd := range chDocs {
		if cd.ChannelID != 0 {
			activeIDs = append(activeIDs, cd.ChannelID)
		}
	}
	r.Allowlist.SetAll(activeIDs)
	r.currentUnion = desired
}

// ------------------ metrics refresher ------------------

type metricsRefresher struct {
	DB        *store.Mongo
	Raw       *tg.Client
	Logger    *log.Logger
	Allowlist *IDAllowlist

	tick time.Duration
}

func newMetricsRefresher(db *store.Mongo, raw *tg.Client, logger *log.Logger, allow *IDAllowlist, tick time.Duration) *metricsRefresher {
	return &metricsRefresher{DB: db, Raw: raw, Logger: logger, Allowlist: allow, tick: tick}
}

func (mr *metricsRefresher) run(ctx context.Context) {
	t := time.NewTicker(mr.tick)
	defer t.Stop()

	// cadence: hot every tick; warm every 6 ticks (~30m if tick=5m); cool every 72 ticks (~6h)
	count := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			count++
			now := time.Now()
			active := mr.Allowlist.Snapshot()
			if len(active) == 0 {
				continue
			}
			// HOT: last 2h
			_ = mr.refreshRange(ctx, active, now.Add(-2*time.Hour), now, 50 /*per ch*/, 1000 /*total*/)
			// WARM: 2h-24h every ~30m
			if count%6 == 0 {
				_ = mr.refreshRange(ctx, active, now.Add(-24*time.Hour), now.Add(-2*time.Hour), 20, 1000)
			}
			// COOL: 1d-7d every ~6h
			if count%72 == 0 {
				_ = mr.refreshRange(ctx, active, now.Add(-7*24*time.Hour), now.Add(-24*time.Hour), 10, 1000)
				count = 0
			}
		}
	}
}

func (mr *metricsRefresher) refreshRange(ctx context.Context, channelIDs []int64, from, to time.Time, perChannel, total int) error {
	// Pull a flat list of recent posts (sorted desc, capped)
	keys, err := mr.DB.FindPostsForRefresh(ctx, channelIDs, from, to, total)
	if err != nil {
		mr.Logger.Printf("refresh: find posts: %v", err)
		return err
	}
	// Group by channel and cap per-channel
	group := map[int64][]int{}
	for _, k := range keys {
		group[k.ChannelID] = append(group[k.ChannelID], k.MsgID)
	}
	for id, arr := range group {
		if len(arr) > perChannel {
			group[id] = arr[:perChannel]
		}
	}

	// Fetch channel meta to build InputChannel
	ids := make([]int64, 0, len(group))
	for id := range group {
		ids = append(ids, id)
	}
	meta, err := mr.DB.GetChannelsByIDs(ctx, ids)
	if err != nil {
		mr.Logger.Printf("refresh: get channels meta: %v", err)
		return err
	}

	// For each channel, chunk message ids and call channels.getMessages
	for cid, mids := range group {
		cd, ok := meta[cid]
		if !ok || cd.AccessHash == 0 {
			continue
		}
		input := &tg.InputChannel{ChannelID: cd.ChannelID, AccessHash: cd.AccessHash}
		// chunk by 90
		for i := 0; i < len(mids); i += 90 {
			end := i + 90
			if end > len(mids) {
				end = len(mids)
			}
			ids := make([]tg.InputMessageClass, 0, end-i)
			for _, m := range mids[i:end] {
				ids = append(ids, &tg.InputMessageID{ID: m})
			}
			resp, err := mr.Raw.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
				Channel: input,
				ID:      ids,
			})
			if err != nil {
				mr.Logger.Printf("refresh: get messages cid=%d: %v", cid, err)
				continue
			}
			// Extract messages and update metrics
			for _, mm := range extractMessages(resp) {
				if mm == nil {
					continue
				}
				var ed *time.Time
				if mm.EditDate != 0 {
					t := time.Unix(int64(mm.EditDate), 0).UTC()
					ed = &t
				}
				var v, f *int
				if mm.Views != 0 {
					v = &mm.Views
				}
				if mm.Forwards != 0 {
					f = &mm.Forwards
				}
				_ = mr.DB.UpdatePostMetrics(ctx, cd.ChannelID, mm.ID, v, f, nil, ed)
			}
			// small delay per chunk
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// small delay per channel
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func extractMessages(ms tg.MessagesMessagesClass) []*tg.Message {
	out := []*tg.Message{}
	switch v := ms.(type) {
	case *tg.MessagesMessages:
		for _, m := range v.Messages {
			if msg, ok := m.(*tg.Message); ok {
				out = append(out, msg)
			}
		}
	case *tg.MessagesMessagesSlice:
		for _, m := range v.Messages {
			if msg, ok := m.(*tg.Message); ok {
				out = append(out, msg)
			}
		}
	case *tg.MessagesChannelMessages:
		for _, m := range v.Messages {
			if msg, ok := m.(*tg.Message); ok {
				out = append(out, msg)
			}
		}
	}
	return out
}

// ------------------ HTTP feed API ------------------

type server struct {
	DB *store.Mongo
}

type channelView struct {
	ID       int64  `json:"id"`
	Username string `json:"username,omitempty"`
	Title    string `json:"title,omitempty"`
	Key      string `json:"key"`
}

type feedItem struct {
	Channel channelView   `json:"channel"`
	Post    store.PostDoc `json:"post"`
}

type feedResponse struct {
	UserID int64      `json:"user_id"`
	Mode   string     `json:"mode"`
	Items  []feedItem `json:"items"`
}

func (s *server) feedHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	q := r.URL.Query()

	userIDStr := q.Get("user_id")
	if userIDStr == "" {
		http.Error(w, "missing user_id", http.StatusBadRequest)
		return
	}
	uid, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid user_id", http.StatusBadRequest)
		return
	}

	mode := q.Get("mode")
	if mode == "" {
		mode = "chrono"
	}
	hours := 48
	if h := q.Get("hours"); h != "" {
		if v, err := strconv.Atoi(h); err == nil && v > 0 && v <= 168 {
			hours = v
		}
	}
	limit := 100
	if l := q.Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 500 {
			limit = v
		}
	}
	since := time.Now().Add(-time.Duration(hours) * time.Hour)

	// user subs (raw inputs)
	rawSubs, err := s.DB.GetUserSubscriptions(ctx, uid)
	if err != nil {
		http.Error(w, "db error: get subs", http.StatusInternalServerError)
		return
	}
	if len(rawSubs) == 0 {
		_ = json.NewEncoder(w).Encode(feedResponse{UserID: uid, Mode: mode, Items: []feedItem{}})
		return
	}
	// canonical keys
	keys := make([]string, 0, len(rawSubs))
	for _, in := range rawSubs {
		if pi, err := parseChannelInput(in); err == nil {
			keys = append(keys, pi.Key)
		}
	}
	chDocs, err := s.DB.GetChannelsByInputs(ctx, keys)
	if err != nil {
		http.Error(w, "db error: map channels", http.StatusInternalServerError)
		return
	}

	var channelIDs []int64
	for _, k := range keys {
		if cd, ok := chDocs[k]; ok && cd.ChannelID != 0 {
			channelIDs = append(channelIDs, cd.ChannelID)
		}
	}
	if len(channelIDs) == 0 {
		_ = json.NewEncoder(w).Encode(feedResponse{UserID: uid, Mode: mode, Items: []feedItem{}})
		return
	}

	posts, err := s.DB.FindRecentPostsByChannels(ctx, channelIDs, since, limit)
	if err != nil {
		http.Error(w, "db error: find posts", http.StatusInternalServerError)
		return
	}

	metaByID, err := s.DB.GetChannelsByIDs(ctx, channelIDs)
	if err != nil {
		http.Error(w, "db error: channels meta", http.StatusInternalServerError)
		return
	}

	items := make([]feedItem, 0, len(posts))
	for _, p := range posts {
		cv := channelView{ID: p.ChannelID, Key: ""}
		if cd, ok := metaByID[p.ChannelID]; ok {
			cv.Title = cd.Title
			cv.Username = cd.Username
			cv.Key = cd.Key
		}
		items = append(items, feedItem{Channel: cv, Post: p})
	}

	resp := feedResponse{UserID: uid, Mode: mode, Items: items}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// ------------------ main ------------------

func mustGet(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing env %s", key)
	}
	return v
}

func main() {
	_ = godotenv.Load()

	appIDStr := mustGet("APP_ID")
	appHash := mustGet("APP_HASH")
	mongoURI := mustGet("MONGO_URI")

	appID64, err := strconv.ParseInt(appIDStr, 10, 32)
	if err != nil {
		log.Fatalf("APP_ID must be integer: %v", err)
	}
	appID := int(appID64)

	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		dbName = "tgfeed"
	}

	httpAddr := os.Getenv("FEED_HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}

	reconcileEvery := 60 * time.Second
	if v := os.Getenv("RECONCILE_INTERVAL_SEC"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 10 && i <= 600 {
			reconcileEvery = time.Duration(i) * time.Second
		}
	}

	refreshTick := 5 * time.Minute
	if v := os.Getenv("REFRESH_EVERY_SEC"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 60 && i <= 3600 {
			refreshTick = time.Duration(i) * time.Second
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Mongo
	db, err := store.Connect(ctx, mongoURI, dbName)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	log.Println("Mongo connected.")

	// Allowlist
	allow := NewIDAllowlist()

	// Ingest
	h := &ingest.Handler{DB: db, Allowed: allow}

	// Telegram client
	client := telegram.NewClient(appID, appHash, telegram.Options{
		UpdateHandler: telegram.UpdateHandlerFunc(h.Handle),
	})

	// HTTP server
	srv := &server{DB: db}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/feed", srv.feedHandler)
	httpSrv := &http.Server{Addr: httpAddr, Handler: mux}

	errCh := make(chan error, 2)

	// Start HTTP
	go func() {
		log.Printf("HTTP feed server listening on %s", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	// Start Telegram client + reconciler + refresher
	go func() {
		errCh <- client.Run(ctx, func(ctx context.Context) error {
			authFlow := auth.NewFlow(newStdinAuth(), auth.SendCodeOptions{
				AllowFlashCall: false,
				CurrentNumber:  true,
			})
			if err := client.Auth().IfNecessary(ctx, authFlow); err != nil {
				return fmt.Errorf("authorization failed: %w", err)
			}
			self, _ := client.Self(ctx)
			log.Printf("Authorized as %s %s (id=%d)", self.FirstName, self.LastName, self.ID)

			raw := tg.NewClient(client)

			// reconciler -> maintains membership and allowlist
			recon := newReconciler(db, raw, log.Default(), allow)
			go recon.run(ctx, reconcileEvery)

			// metrics refresher
			ref := newMetricsRefresher(db, raw, log.Default(), allow, refreshTick)
			go ref.run(ctx)

			log.Println("Listening for channel posts...")
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Printf("fatal: %v", err)
	}

	_ = httpSrv.Shutdown(context.Background())
	log.Println("Shutting down.")
}
