package ingest

import (
	"context"
	"time"

	"example.com/feed-aggregator/internal/store"
	"github.com/gotd/td/tg"
)

// Allowlist interface (provided by main)
type IDAllowlist interface {
	Has(id int64) bool
}

type Handler struct {
	DB      *store.Mongo
	Allowed IDAllowlist // <- injected allowlist
}

func (h *Handler) Handle(ctx context.Context, u tg.UpdatesClass) error {
	switch upd := u.(type) {
	case *tg.Updates:
		for _, x := range upd.Updates {
			_ = h.handleUpdate(ctx, x)
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
		if msg, ok := up.Message.(*tg.Message); ok {
			return h.handleChannelMessage(ctx, msg)
		}
	case *tg.UpdateEditChannelMessage:
		if msg, ok := up.Message.(*tg.Message); ok {
			return h.handleChannelMessageEdit(ctx, msg)
		}
	}
	return nil
}

func (h *Handler) handleChannelMessage(ctx context.Context, msg *tg.Message) error {
	peer := msg.GetPeerID()
	ch, ok := peer.(*tg.PeerChannel)
	if !ok {
		return nil
	}
	// --- Only save if this channel is in our allowlist ---
	if h.Allowed != nil && !h.Allowed.Has(int64(ch.ChannelID)) {
		return nil
	}

	doc := store.PostDoc{
		ChannelID: int64(ch.ChannelID),
		MsgID:     msg.ID,
		Date:      time.Unix(int64(msg.Date), 0).UTC(),
		Text:      msg.Message,
	}

	// Entities
	if len(msg.Entities) > 0 {
		doc.Entities = toEntities(msg.Entities)
	}
	// Signature & via bot
	if msg.PostAuthor != "" {
		doc.Signature = msg.PostAuthor
	}
	if msg.ViaBotID != 0 {
		v := int64(msg.ViaBotID)
		doc.ViaBotID = &v
	}
	// Reply & forward
	if msg.ReplyTo != nil {
		if r, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok && r.ReplyToMsgID != 0 {
			v := int(r.ReplyToMsgID)
			doc.ReplyToMsgID = &v
		}
	}
	if (msg.FwdFrom != tg.MessageFwdHeader{}) {
		doc.FwdFrom = toForwardInfo(&msg.FwdFrom)
	}
	// Grouped albums
	if msg.GroupedID != 0 {
		gid := int64(msg.GroupedID)
		doc.GroupedID = &gid
	}
	// Media
	doc.Media = toMedia(msg.Media)
	// Stats
	if msg.Views != 0 {
		doc.Views = msg.Views
	}
	if msg.Forwards != 0 {
		doc.Forwards = msg.Forwards
	}
	doc.InsertedAt = time.Now().UTC()
	_ = h.DB.UpsertPost(ctx, doc)
	return nil
}

func (h *Handler) handleChannelMessageEdit(ctx context.Context, msg *tg.Message) error {
	peer := msg.GetPeerID()
	ch, ok := peer.(*tg.PeerChannel)
	if !ok {
		return nil
	}
	// --- Ignore edits from channels we don't track ---
	if h.Allowed != nil && !h.Allowed.Has(int64(ch.ChannelID)) {
		return nil
	}
	var ed *time.Time
	if msg.EditDate != 0 {
		t := time.Unix(int64(msg.EditDate), 0).UTC()
		ed = &t
	}
	var v, f *int
	if msg.Views != 0 {
		v = &msg.Views
	}
	if msg.Forwards != 0 {
		f = &msg.Forwards
	}
	_ = h.DB.UpdatePostMetrics(ctx, int64(ch.ChannelID), msg.ID, v, f, nil, ed)
	return nil
}

// -------- helpers: entities, forward, media mapping ----------

func toEntities(es []tg.MessageEntityClass) []store.MessageEntity {
	out := make([]store.MessageEntity, 0, len(es))
	for _, e := range es {
		switch v := e.(type) {
		case *tg.MessageEntityBold:
			out = append(out, store.MessageEntity{Type: "bold", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityItalic:
			out = append(out, store.MessageEntity{Type: "italic", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityCode:
			out = append(out, store.MessageEntity{Type: "code", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityPre:
			out = append(out, store.MessageEntity{Type: "pre", Offset: v.Offset, Length: v.Length, Language: v.Language})
		case *tg.MessageEntityUnderline:
			out = append(out, store.MessageEntity{Type: "underline", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityStrike:
			out = append(out, store.MessageEntity{Type: "strikethrough", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntitySpoiler:
			out = append(out, store.MessageEntity{Type: "spoiler", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityURL:
			out = append(out, store.MessageEntity{Type: "url", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityTextURL:
			out = append(out, store.MessageEntity{Type: "text_url", Offset: v.Offset, Length: v.Length, URL: v.URL})
		case *tg.MessageEntityMention:
			out = append(out, store.MessageEntity{Type: "mention", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityHashtag:
			out = append(out, store.MessageEntity{Type: "hashtag", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityBotCommand:
			out = append(out, store.MessageEntity{Type: "bot_command", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityCustomEmoji:
			out = append(out, store.MessageEntity{Type: "custom_emoji", Offset: v.Offset, Length: v.Length, EmojiID: v.DocumentID})
		case *tg.MessageEntityCashtag:
			out = append(out, store.MessageEntity{Type: "cashtag", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityPhone:
			out = append(out, store.MessageEntity{Type: "phone", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityEmail:
			out = append(out, store.MessageEntity{Type: "email", Offset: v.Offset, Length: v.Length})
		case *tg.MessageEntityBlockquote:
			out = append(out, store.MessageEntity{Type: "blockquote", Offset: v.Offset, Length: v.Length})
		}
	}
	return out
}

func toForwardInfo(f *tg.MessageFwdHeader) *store.ForwardInfo {
	if f == nil {
		return nil
	}
	fi := &store.ForwardInfo{
		FromName:   f.FromName,
		PostAuthor: f.PostAuthor,
		Date:       int64(f.Date),
	}
	if f.FromID != nil {
		switch pid := f.FromID.(type) {
		case *tg.PeerUser:
			fi.FromUserID = int64(pid.UserID)
		case *tg.PeerChannel:
			fi.FromChannelID = int64(pid.ChannelID)
		}
	}
	if f.ChannelPost != 0 {
		fi.FromMessageID = int(f.ChannelPost)
	}
	return fi
}

func toMedia(m tg.MessageMediaClass) *store.Media {
	if m == nil {
		return nil
	}
	switch mm := m.(type) {
	case *tg.MessageMediaPhoto:
		return mapPhoto(mm)
	case *tg.MessageMediaDocument:
		return mapDocument(mm)
	case *tg.MessageMediaWebPage:
		return mapWebPage(mm)
	case *tg.MessageMediaPoll:
		return mapPoll(mm)
	case *tg.MessageMediaGeo:
		return mapGeo(mm)
	case *tg.MessageMediaGeoLive:
		// for now treat like Geo (live metadata omitted)
		return mapGeo(&tg.MessageMediaGeo{Geo: mm.Geo})
	default:
		return &store.Media{Kind: "none"}
	}
}

func mapPhoto(mm *tg.MessageMediaPhoto) *store.Media {
	if ph, ok := mm.Photo.(*tg.Photo); ok {
		mp := &store.MediaPhoto{HasStickers: ph.HasStickers}
		for _, sz := range ph.Sizes {
			switch s := sz.(type) {
			case *tg.PhotoSize:
				mp.Sizes = append(mp.Sizes, store.PhotoSize{W: s.W, H: s.H, Type: s.Type})
			case *tg.PhotoCachedSize:
				mp.Sizes = append(mp.Sizes, store.PhotoSize{W: s.W, H: s.H, Type: s.Type, Bytes: s.Bytes})
			case *tg.PhotoStrippedSize:
				mp.Sizes = append(mp.Sizes, store.PhotoSize{W: 0, H: 0, Type: "stripped", Bytes: s.Bytes})
			}
		}
		return &store.Media{Kind: "photo", Photo: mp}
	}
	return &store.Media{Kind: "photo"}
}

func mapDocument(mm *tg.MessageMediaDocument) *store.Media {
	doc, ok := mm.Document.(*tg.Document)
	if !ok {
		return &store.Media{Kind: "document"}
	}

	thumbs := make([]store.PhotoSize, 0, len(doc.Thumbs))
	for _, t := range doc.Thumbs {
		switch ts := t.(type) {
		case *tg.PhotoSize:
			thumbs = append(thumbs, store.PhotoSize{W: ts.W, H: ts.H, Type: ts.Type})
		case *tg.PhotoCachedSize:
			thumbs = append(thumbs, store.PhotoSize{W: ts.W, H: ts.H, Type: ts.Type, Bytes: ts.Bytes})
		case *tg.PhotoStrippedSize:
			thumbs = append(thumbs, store.PhotoSize{W: 0, H: 0, Type: "stripped", Bytes: ts.Bytes})
		}
	}

	f := store.FileRef{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
		DC:            doc.DCID,
		MimeType:      doc.MimeType,
		Size:          int64(doc.Size),
	}

	// Attribute routing
	var (
		isVideo   bool
		isVoice   bool
		isAudio   bool
		isAnim    bool
		isSticker bool
	)

	media := &store.Media{Kind: "document"} // fallback

	attrs := doc.Attributes
	for _, a := range attrs {
		switch at := a.(type) {
		case *tg.DocumentAttributeVideo:
			isVideo = true
			mv := &store.MediaVideo{
				File:     f,
				Duration: int(at.Duration),
				W:        at.W,
				H:        at.H,
				Round:    at.RoundMessage,
				Thumbs:   thumbs,
			}
			media = &store.Media{Kind: "video", Video: mv}
		case *tg.DocumentAttributeAnimated:
			isAnim = true
			ma := &store.MediaAnimation{
				File:   f,
				Thumbs: thumbs,
			}
			media = &store.Media{Kind: "animation", Animation: ma}
		case *tg.DocumentAttributeAudio:
			if at.Voice {
				isVoice = true
				mv := &store.MediaVoice{
					File:     f,
					Duration: at.Duration,
					// Waveform not in attribute; may be elsewhere; keep for future
				}
				media = &store.Media{Kind: "voice", Voice: mv}
			} else {
				isAudio = true
				ma := &store.MediaAudio{
					File:      f,
					Duration:  at.Duration,
					Title:     at.Title,
					Performer: at.Performer,
				}
				media = &store.Media{Kind: "audio", Audio: ma}
			}
		case *tg.DocumentAttributeSticker:
			isSticker = true
			ms := &store.MediaSticker{
				File:   f,
				Alt:    at.Alt,
				Thumbs: thumbs,
				// Animated/video flags are separate attrs; we mark from attributes set
			}
			// detect animated/video sticker hints
			for _, a2 := range attrs {
				switch a2.(type) {
				case *tg.DocumentAttributeAnimated:
					ms.Animated = true
				case *tg.DocumentAttributeVideo:
					ms.Video = true
				}
			}
			media = &store.Media{Kind: "sticker", Sticker: ms}
		case *tg.DocumentAttributeHasStickers:
			// not needed for previewing; skip
		case *tg.DocumentAttributeFilename:
			// we already copied FileName into FileRef if you want — optional
			f.FileName = at.FileName
		}
	}

	// Default cases: if MIME says image/* but no Photo, it’s an image document (rare). Keep as document.
	if isVideo || isAnim || isAudio || isVoice || isSticker {
		return media
	}
	// Fallback to generic document with thumbs retained
	return media
}

func mapWebPage(mm *tg.MessageMediaWebPage) *store.Media {
	wp := &store.WebPagePreview{}
	if page, ok := mm.Webpage.(*tg.WebPage); ok {
		wp.URL = page.URL
		wp.DisplayURL = page.DisplayURL
		wp.SiteName = page.SiteName
		wp.Title = page.Title
		wp.Description = page.Description
		if ph, ok := page.Photo.(*tg.Photo); ok {
			mp := &store.MediaPhoto{HasStickers: ph.HasStickers}
			for _, sz := range ph.Sizes {
				switch s := sz.(type) {
				case *tg.PhotoSize:
					mp.Sizes = append(mp.Sizes, store.PhotoSize{W: s.W, H: s.H, Type: s.Type})
				case *tg.PhotoCachedSize:
					mp.Sizes = append(mp.Sizes, store.PhotoSize{W: s.W, H: s.H, Type: s.Type, Bytes: s.Bytes})
				case *tg.PhotoStrippedSize:
					mp.Sizes = append(mp.Sizes, store.PhotoSize{W: 0, H: 0, Type: "stripped", Bytes: s.Bytes})
				}
			}
			wp.Photo = mp
		}
		return &store.Media{Kind: "webpage", WebPage: wp}
	}
	return &store.Media{Kind: "webpage"}
}

func mapPoll(mm *tg.MessageMediaPoll) *store.Media {
	p := &store.MediaPoll{}
	pl := mm.Poll
	p.ID = pl.ID
	p.Question = pl.Question.Text
	p.Closed = pl.Closed
	p.PublicVoters = pl.PublicVoters
	p.MultipleChoice = pl.MultipleChoice
	p.Quiz = pl.Quiz
	for _, ans := range pl.Answers {
		p.Options = append(p.Options, ans.Text.Text)
	}
	// We skip results counts for now; can be added from mm.Results if needed.
	return &store.Media{Kind: "poll", Poll: p}
}

func mapGeo(mm *tg.MessageMediaGeo) *store.Media {
	g := &store.MediaGeo{}
	if gp, ok := mm.Geo.(*tg.GeoPoint); ok {
		g.Lat = gp.Lat
		g.Lon = gp.Long
		g.AccuracyRadius = gp.AccuracyRadius
	}
	return &store.Media{Kind: "geo", Geo: g}
}
