package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"example.com/feed-aggregator/internal/ingest"
	"example.com/feed-aggregator/internal/store"

	"github.com/joho/godotenv"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
)

// mustGet reads an env var or exits with a clear error.
func mustGet(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing env %s", key)
	}
	return v
}

// ----- Minimal stdin authenticator (implements auth.CodeAuthenticator) -----

type stdinAuth struct {
	in  *bufio.Reader
	out *log.Logger

	// Optional overrides via env to avoid prompting every time
	phoneEnv    string
	codeEnv     string
	passwordEnv string
}

// SignUp implements auth.UserAuthenticator.
func (a *stdinAuth) SignUp(ctx context.Context) (auth.UserInfo, error) {
	panic("unimplemented")
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

func (a *stdinAuth) readLine(prompt string, fallback string) (string, error) {
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
	return a.readLine("Enter phone (with +): ", a.phoneEnv)
}

func (a *stdinAuth) Code(ctx context.Context, sentCode *tg.AuthSentCode) (string, error) {
	return a.readLine("Enter code: ", a.codeEnv)
}

func (a *stdinAuth) Password(ctx context.Context) (string, error) {
	// Called if you have 2FA password enabled.
	return a.readLine("Enter 2FA password: ", a.passwordEnv)
}

func (a *stdinAuth) AcceptTermsOfService(ctx context.Context, tos tg.HelpTermsOfService) error {
	a.out.Printf("You must accept ToS to continue: %s\n", tos.Text)
	a.out.Print("Type 'yes' to accept: ")
	line, err := a.in.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(strings.ToLower(line)) != "yes" {
		return fmt.Errorf("terms of service not accepted")
	}
	return nil
}

// --------------------------------------------------------------------------

func main() {
	// Load .env if present (safe no-op if absent).
	_ = godotenv.Load()

	// --- Config & env parsing ---
	appIDStr := mustGet("APP_ID")
	appHash := mustGet("APP_HASH")
	mongoURI := mustGet("MONGO_URI")

	// Parse APP_ID (must be integer).
	appID64, err := strconv.ParseInt(appIDStr, 10, 32)
	if err != nil {
		log.Fatalf("APP_ID must be integer: %v", err)
	}
	appID := int(appID64)

	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		dbName = "tgfeed"
	}

	// Optional: comma-separated @usernames to pre-join on startup.
	var prejoinList []string
	if s := strings.TrimSpace(os.Getenv("PREJOIN_CHANNELS")); s != "" {
		for _, u := range strings.Split(s, ",") {
			u = strings.TrimSpace(u)
			if u != "" {
				prejoinList = append(prejoinList, u)
			}
		}
	}

	// --- Context & signals ---
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// --- DB connect ---
	db, err := store.Connect(ctx, mongoURI, dbName)
	if err != nil {
		log.Fatalf("mongo connect failed: %v", err)
	}
	log.Println("Mongo connected.")

	// --- Updates handler wired into telegram client ---
	h := &ingest.Handler{DB: db}

	// Build ONE telegram client (no session storage helpers used here).
	client := telegram.NewClient(appID, appHash, telegram.Options{
		UpdateHandler: telegram.UpdateHandlerFunc(h.Handle),
	})

	// Run the client. This blocks until ctx is cancelled (Ctrl-C).
	if err := client.Run(ctx, func(ctx context.Context) error {
		// If not authorized yet, perform SMS/2FA auth using our stdin authenticator.
		authFlow := auth.NewFlow(
			newStdinAuth(), // our authenticator
			auth.SendCodeOptions{
				AllowFlashCall: false,
				CurrentNumber:  true,
			},
		)
		if err := client.Auth().IfNecessary(ctx, authFlow); err != nil {
			return fmt.Errorf("authorization failed: %w", err)
		}

		self, _ := client.Self(ctx)
		log.Printf("Authorized as %s %s (id=%d)", self.FirstName, self.LastName, self.ID)

		// Build tg client bound to the running *telegram.Client.
		raw := tg.NewClient(client)

		// (Optional) Pre-join public channels by @username on boot.
		if len(prejoinList) > 0 {
			for _, uname := range prejoinList {
				if err := ensureJoinByUsername(ctx, raw, uname); err != nil {
					log.Printf("pre-join '%s' failed: %v", uname, err)
				} else {
					log.Printf("pre-joined %s", uname)
				}
				// small delay to be polite & avoid flood waits
				select {
				case <-time.After(500 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		log.Println("Listening for channel posts...")
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		// If Run returns an error unrelated to context cancellation, log it.
		if ctx.Err() == nil {
			log.Fatalf("client run failed: %v", err)
		}
	}

	log.Println("Shutting down.")
}

// ensureJoinByUsername joins a public channel/supergroup by @username.
func ensureJoinByUsername(ctx context.Context, raw *tg.Client, username string) error {
	if username == "" {
		return fmt.Errorf("empty username")
	}
	if username[0] == '@' {
		username = username[1:]
	}

	// Resolve @username -> peer/entities
	res, err := raw.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{
		Username: username,
	})
	if err != nil {
		return fmt.Errorf("resolve '%s': %w", username, err)
	}

	// Find the channel entity in the result.
	var ch *tg.Channel
	for _, chat := range res.Chats {
		if c, ok := chat.(*tg.Channel); ok {
			ch = c
			break
		}
	}
	if ch == nil {
		return fmt.Errorf("'%s' did not resolve to a channel/supergroup", username)
	}

	input := &tg.InputChannel{
		ChannelID:  ch.ID,
		AccessHash: ch.AccessHash,
	}
	_, err = raw.ChannelsJoinChannel(ctx, input)
	return err
}
