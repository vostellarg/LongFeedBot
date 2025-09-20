package ingest

import (
	"context"
	"time"

	"example.com/feed-aggregator/internal/store"
	"github.com/gotd/td/tg"
)

type Handler struct {
	DB *store.Mongo
}

func (h *Handler) Handle(ctx context.Context, u tg.UpdatesClass) error {
	switch upd := u.(type) {
	case *tg.Updates:
		for _, x := range upd.Updates {
			if err := h.handleUpdate(ctx, x); err != nil {
				// log and continue
			}
		}
	case *tg.UpdateShort:
		// Ignore
	case *tg.UpdatesCombined:
		for _, x := range upd.Updates {
			_ = h.handleUpdate(ctx, x)
		}
	}
	return nil
}

func (h *Handler) handleUpdate(ctx context.Context, u tg.UpdateClass) error {
	switch up := u.(type) {
	case *tg.UpdateNewChannelMessage:
		// This is a new message in a channel/supergroup. :contentReference[oaicite:5]{index=5}
		if msg, ok := up.Message.(*tg.Message); ok {
			peer := msg.GetPeerID()
			ch, ok := peer.(*tg.PeerChannel)
			if !ok {
				return nil
			}
			doc := store.PostDoc{
				ChannelID: ch.ChannelID,
				MsgID:     msg.ID,
				Date:      time.Unix(int64(msg.Date), 0).UTC(),
				Text:      msg.Message,
			}
			// If available, these fields will be non-zero. (Channel posts expose views/forwards.) :contentReference[oaicite:6]{index=6}
			if msg.Views != 0 {
				doc.Views = msg.Views
			}
			if msg.Forwards != 0 {
				doc.Forwards = msg.Forwards
			}
			// Reactions summary (if enabled on channel) lives under msg.Reactions.
			// You can map msg.Reactions.RecentReactions / Results to doc.Reactions here.

			_ = h.DB.UpsertPost(ctx, doc)
		}
	}
	return nil
}
