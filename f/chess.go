package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ==================== CHESS MODELS ====================

type ChessRating struct {
	UserID     primitive.ObjectID `bson:"user_id" json:"user_id"`
	Username   string             `bson:"username" json:"username"`
	Rating     int                `bson:"rating" json:"rating"`
	Games      int                `bson:"games" json:"games"`
	Wins       int                `bson:"wins" json:"wins"`
	Losses     int                `bson:"losses" json:"losses"`
	Draws      int                `bson:"draws" json:"draws"`
	LastPlayed time.Time          `bson:"last_played" json:"last_played"`
	CreatedAt  time.Time          `bson:"created_at" json:"created_at"`
}

type ChessRoom struct {
	ID          primitive.ObjectID  `bson:"_id,omitempty" json:"id"`
	Name        string              `bson:"name" json:"name"`
	HostID      primitive.ObjectID  `bson:"host_id" json:"host_id"`
	HostName    string              `bson:"host_name" json:"host_name"`
	HostRating  int                 `bson:"host_rating" json:"host_rating"`
	HostColor   string              `bson:"host_color" json:"host_color"` // "w" or "b"
	TimeControl TimeControl         `bson:"time_control" json:"time_control"`
	Status      string              `bson:"status" json:"status"` // "waiting", "playing", "finished"
	GuestID     *primitive.ObjectID `bson:"guest_id,omitempty" json:"guest_id,omitempty"`
	GuestName   string              `bson:"guest_name,omitempty" json:"guest_name,omitempty"`
	GuestRating int                 `bson:"guest_rating,omitempty" json:"guest_rating,omitempty"`
	CreatedAt   time.Time           `bson:"created_at" json:"created_at"`
	StartedAt   *time.Time          `bson:"started_at,omitempty" json:"started_at,omitempty"`
	ExpiresAt   time.Time           `bson:"expires_at" json:"expires_at"`
}

type TimeControl struct {
	BaseMs     int `bson:"base_ms" json:"base_ms"`
	IncrementS int `bson:"increment_s" json:"increment_s"`
}

type ChessGame struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	RoomID      primitive.ObjectID `bson:"room_id" json:"room_id"`
	WhiteID     primitive.ObjectID `bson:"white_id" json:"white_id"`
	WhiteName   string             `bson:"white_name" json:"white_name"`
	BlackID     primitive.ObjectID `bson:"black_id" json:"black_id"`
	BlackName   string             `bson:"black_name" json:"black_name"`
	FEN         string             `bson:"fen" json:"fen"`
	Turn        string             `bson:"turn" json:"turn"` // "w" or "b"
	Moves       []ChessMove        `bson:"moves" json:"moves"`
	Status      string             `bson:"status" json:"status"` // "active", "checkmate", "stalemate", "draw", "resigned", "timeout"
	Winner      string             `bson:"winner,omitempty" json:"winner,omitempty"` // "w", "b", or "draw"
	WhiteTime   int64              `bson:"white_time_ms" json:"white_time_ms"`
	BlackTime   int64              `bson:"black_time_ms" json:"black_time_ms"`
	StartedAt   time.Time          `bson:"started_at" json:"started_at"`
	FinishedAt  *time.Time         `bson:"finished_at,omitempty" json:"finished_at,omitempty"`
	LastMoveAt  *time.Time         `bson:"last_move_at,omitempty" json:"last_move_at,omitempty"`
	TimeControl TimeControl        `bson:"time_control" json:"time_control"`
}

type ChessMove struct {
	Number    int       `bson:"number" json:"number"`
	From      string    `bson:"from" json:"from"`
	To        string    `bson:"to" json:"to"`
	Piece     string    `bson:"piece" json:"piece"`
	Captured  string    `bson:"captured,omitempty" json:"captured,omitempty"`
	Promotion string    `bson:"promotion,omitempty" json:"promotion,omitempty"`
	SAN       string    `bson:"san" json:"san"`
	FEN       string    `bson:"fen" json:"fen"`
	Timestamp time.Time `bson:"timestamp" json:"timestamp"`
}

// ==================== WEBSOCKET HUB ====================

type ChessHub struct {
	mu         sync.RWMutex
	rooms      map[primitive.ObjectID]*RoomSession
	clients    map[*websocket.Conn]*ChessClient
	broadcast  chan BroadcastMsg
	register   chan *ChessClient
	unregister chan *ChessClient
}

type ChessClient struct {
	hub      *ChessHub
	conn     *websocket.Conn
	send     chan []byte
	userID   primitive.ObjectID
	username string
	rating   int
	roomID   primitive.ObjectID
}

type RoomSession struct {
	Room       *ChessRoom
	Game       *ChessGame
	WhiteConn  *websocket.Conn
	BlackConn  *websocket.Conn
	Spectators []*websocket.Conn
	ClockTick  chan struct{}
	Closed     bool
	mu         sync.RWMutex
}

type BroadcastMsg struct {
	RoomID primitive.ObjectID
	Data   []byte
}

var chessHub = &ChessHub{
	rooms:      make(map[primitive.ObjectID]*RoomSession),
	clients:    make(map[*websocket.Conn]*ChessClient),
	broadcast:  make(chan BroadcastMsg, 1000),
	register:   make(chan *ChessClient),
	unregister: make(chan *ChessClient),
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // در production محدود شود
	},
}

// ==================== CHESS ENGINE (Simple) ====================

const StartFEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"

type Board [64]string // index 0-63, "" = empty, e.g. "wK", "bQ"

func parseFEN(fen string) (Board, string, error) {
	var board Board
	parts := strings.Fields(fen)
	if len(parts) < 2 {
		return board, "w", fmt.Errorf("invalid FEN")
	}

	ranks := strings.Split(parts[0], "/")
	if len(ranks) != 8 {
		return board, "w", fmt.Errorf("invalid FEN ranks")
	}

	for r, rank := range ranks {
		file := 0
		for _, ch := range rank {
			if ch >= '1' && ch <= '8' {
				file += int(ch - '0')
			} else {
				color := "w"
				if ch >= 'a' && ch <= 'z' {
					color = "b"
					ch = ch - 'a' + 'A'
				}
				idx := r*8 + file
				if idx < 64 {
					board[idx] = color + string(ch)
				}
				file++
			}
		}
	}

	turn := "w"
	if parts[1] == "b" {
		turn = "b"
	}
	return board, turn, nil
}

func boardToFEN(board Board, turn string, moveNum int) string {
	var fen strings.Builder
	for r := 0; r < 8; r++ {
		empty := 0
		for f := 0; f < 8; f++ {
			piece := board[r*8+f]
			if piece == "" {
				empty++
			} else {
				if empty > 0 {
					fen.WriteString(fmt.Sprintf("%d", empty))
					empty = 0
				}
				ch := piece[1]
				if piece[0] == 'b' {
					ch = ch - 'A' + 'a'
				}
				fen.WriteByte(ch)
			}
		}
		if empty > 0 {
			fen.WriteString(fmt.Sprintf("%d", empty))
		}
		if r < 7 {
			fen.WriteByte('/')
		}
	}
	fen.WriteString(fmt.Sprintf(" %s - - 0 %d", turn, moveNum))
	return fen.String()
}

// ==================== HUB LOGIC ====================

func (h *ChessHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.conn] = client
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.conn]; ok {
				delete(h.clients, client.conn)
				close(client.send)
				h.handleDisconnect(client)
			}
			h.mu.Unlock()

		case msg := <-h.broadcast:
			h.mu.RLock()
			session, exists := h.rooms[msg.RoomID]
			h.mu.RUnlock()
			if !exists {
				continue
			}
			session.mu.RLock()
			recipients := []*websocket.Conn{}
			if session.WhiteConn != nil {
				recipients = append(recipients, session.WhiteConn)
			}
			if session.BlackConn != nil {
				recipients = append(recipients, session.BlackConn)
			}
			recipients = append(recipients, session.Spectators...)
			session.mu.RUnlock()

			for _, conn := range recipients {
				select {
				case h.clients[conn].send <- msg.Data:
				default:
					h.unregister <- h.clients[conn]
				}
			}
		}
	}
}

func (h *ChessHub) handleDisconnect(client *ChessClient) {
	if client.roomID.IsZero() {
		return
	}
	h.mu.RLock()
	session, exists := h.rooms[client.roomID]
	h.mu.RUnlock()
	if !exists {
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.Closed {
		return
	}

	// اگر بازی فعال است، حریف برنده می‌شود (disconnect)
	if session.Game != nil && session.Game.Status == "active" {
		winner := "b"
		if client.conn == session.BlackConn {
			winner = "w"
		}
		session.Game.Status = "resigned"
		session.Game.Winner = winner
		now := time.Now().UTC()
		session.Game.FinishedAt = &now

		// ذخیره در دیتابیس
		go saveGame(session.Game)
		go updateRatings(session.Game)

		// ارسال پیام پایان بازی
		msg, _ := json.Marshal(map[string]interface{}{
			"type":   "game_over",
			"reason": "disconnect",
			"winner": winner,
		})
		h.broadcast <- BroadcastMsg{RoomID: client.roomID, Data: msg}

		// پاک کردن session
		session.Closed = true
		delete(h.rooms, client.roomID)
	}
}

// ==================== CHESS HANDLERS ====================

// GET /chess/stats - دریافت آمار شطرنج کاربر
func getChessStats(c *gin.Context) {
	uVal, exists := c.Get("user")
	if !exists {
		c.JSON(401, gin.H{"msg": "Unauthorized"})
		return
	}
	user := uVal.(User)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rating ChessRating
	err := db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": user.ID}).Decode(&rating)
	if err == mongo.ErrNoDocuments {
		// ساخت rating پیش‌فرض
		rating = ChessRating{
			UserID:    user.ID,
			Username:  user.Username,
			Rating:    1200,
			CreatedAt: time.Now().UTC(),
		}
		db.Collection("chess_ratings").InsertOne(ctx, rating)
	} else if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}

	c.JSON(200, gin.H{
		"rating":    rating.Rating,
		"games":     rating.Games,
		"wins":      rating.Wins,
		"losses":    rating.Losses,
		"draws":     rating.Draws,
		"username":  rating.Username,
	})
}

// POST /chess/rooms - ساخت روم جدید
func createChessRoom(c *gin.Context) {
	uVal, exists := c.Get("user")
	if !exists {
		c.JSON(401, gin.H{"msg": "Unauthorized"})
		return
	}
	user := uVal.(User)

	var body struct {
		Name        string `json:"name" binding:"required"`
		BaseMs      int    `json:"base_ms"`
		IncrementS  int    `json:"increment_s"`
		HostColor   string `json:"host_color"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Invalid request"})
		return
	}

	if body.BaseMs < 60000 || body.BaseMs > 3600000 {
		body.BaseMs = 600000 // 10 دقیقه پیش‌فرض
	}
	if body.HostColor != "w" && body.HostColor != "b" {
		if rand.Float32() < 0.5 {
			body.HostColor = "w"
		} else {
			body.HostColor = "b"
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// دریافت rating کاربر (خواندن از کلکسیون شطرنج)
	var rating ChessRating
	err := db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": user.ID}).Decode(&rating)
	if err != nil {
		rating = ChessRating{Rating: 1200}
	}

	// بررسی اینکه کاربر در حال حاضر در روم فعال نباشد
	count, _ := db.Collection("chess_rooms").CountDocuments(ctx, bson.M{
		"host_id": user.ID,
		"status":  "waiting",
	})
	if count > 0 {
		c.JSON(409, gin.H{"msg": "You already have an active room"})
		return
	}

	room := ChessRoom{
		Name:       body.Name,
		HostID:     user.ID,
		HostName:   user.Username,
		HostRating: rating.Rating,
		HostColor:  body.HostColor,
		TimeControl: TimeControl{
			BaseMs:     body.BaseMs,
			IncrementS: body.IncrementS,
		},
		Status:    "waiting",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(30 * time.Minute),
	}

	res, err := db.Collection("chess_rooms").InsertOne(ctx, room)
	if err != nil {
		c.JSON(500, gin.H{"msg": "Failed to create room"})
		return
	}

	room.ID = res.InsertedID.(primitive.ObjectID)
	c.JSON(201, gin.H{
		"room": room,
		"msg":  "Room created successfully",
	})
}

// GET /chess/rooms - لیست روم‌های فعال
func listChessRooms(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// پاک کردن روم‌های منقضی شده
	db.Collection("chess_rooms").DeleteMany(ctx, bson.M{
		"status":     "waiting",
		"expires_at": bson.M{"$lt": time.Now().UTC()},
	})

	filter := bson.M{"status": "waiting"}
	opts := options.Find().SetSort(bson.M{"created_at": -1}).SetLimit(50)

	cursor, err := db.Collection("chess_rooms").Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var rooms []ChessRoom
	if err := cursor.All(ctx, &rooms); err != nil {
		c.JSON(500, gin.H{"msg": "Read error"})
		return
	}

	if rooms == nil {
		rooms = []ChessRoom{}
	}

	c.JSON(200, gin.H{
		"rooms": rooms,
		"count": len(rooms),
	})
}

// POST /chess/rooms/:room_id/join - پیوستن به روم
func joinChessRoom(c *gin.Context) {
	uVal, exists := c.Get("user")
	if !exists {
		c.JSON(401, gin.H{"msg": "Unauthorized"})
		return
	}
	user := uVal.(User)

	roomID := c.Param("room_id")
	oid, err := primitive.ObjectIDFromHex(roomID)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid room ID"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var room ChessRoom
	err = db.Collection("chess_rooms").FindOne(ctx, bson.M{"_id": oid}).Decode(&room)
	if err == mongo.ErrNoDocuments {
		c.JSON(404, gin.H{"msg": "Room not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}

	if room.Status != "waiting" {
		c.JSON(409, gin.H{"msg": "Room is no longer available"})
		return
	}

	if room.HostID == user.ID {
		c.JSON(400, gin.H{"msg": "You cannot join your own room"})
		return
	}

	// دریافت rating کاربر
	var rating ChessRating
	err = db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": user.ID}).Decode(&rating)
	if err != nil {
		rating = ChessRating{Rating: 1200}
	}

	// بروزرسانی روم
	now := time.Now().UTC()
	guestColor := "b"
	if room.HostColor == "b" {
		guestColor = "w"
	}

	_, err = db.Collection("chess_rooms").UpdateOne(ctx,
		bson.M{"_id": oid, "status": "waiting"},
		bson.M{
			"$set": bson.M{
				"status":      "playing",
				"guest_id":    user.ID,
				"guest_name":  user.Username,
				"guest_rating": rating.Rating,
				"started_at":  now,
			},
		},
	)
	if err != nil {
		c.JSON(409, gin.H{"msg": "Room was taken by another player"})
		return
	}

	// ساخت بازی
	var whiteID, blackID primitive.ObjectID
	var whiteName, blackName string
	var whiteRating, blackRating int

	if room.HostColor == "w" {
		whiteID = room.HostID
		whiteName = room.HostName
		whiteRating = room.HostRating
		blackID = user.ID
		blackName = user.Username
		blackRating = rating.Rating
	} else {
		whiteID = user.ID
		whiteName = user.Username
		whiteRating = rating.Rating
		blackID = room.HostID
		blackName = room.HostName
		blackRating = room.HostRating
	}

	game := ChessGame{
		RoomID:    oid,
		WhiteID:   whiteID,
		WhiteName: whiteName,
		BlackID:   blackID,
		BlackName: blackName,
		FEN:       StartFEN,
		Turn:      "w",
		Moves:     []ChessMove{},
		Status:    "active",
		WhiteTime: int64(room.TimeControl.BaseMs),
		BlackTime: int64(room.TimeControl.BaseMs),
		StartedAt: now,
		TimeControl: room.TimeControl,
	}

	res, err := db.Collection("chess_games").InsertOne(ctx, game)
	if err != nil {
		c.JSON(500, gin.H{"msg": "Failed to create game"})
		return
	}
	game.ID = res.InsertedID.(primitive.ObjectID)

	// ساخت RoomSession
	session := &RoomSession{
		Room: &room,
		Game: &game,
		ClockTick: make(chan struct{}),
	}

	chessHub.mu.Lock()
	chessHub.rooms[oid] = session
	chessHub.mu.Unlock()

	// شروع تایمر ساعت
	go runGameClock(oid)

	c.JSON(200, gin.H{
		"msg":   "Joined room successfully",
		"game":  game,
		"color": guestColor,
	})
}

// WebSocket endpoint
func chessWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	// احراز هویت از طریق query parameter یا header
	token := c.Query("token")
	if token == "" {
		token = c.GetHeader("Authorization")
		token = strings.TrimPrefix(token, "Bearer ")
	}

	username, userID, userRating, err := validateChessToken(token)
	if err != nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Invalid token"))
		conn.Close()
		return
	}

	client := &ChessClient{
		hub:      chessHub,
		conn:     conn,
		send:     make(chan []byte, 256),
		userID:   userID,
		username: username,
		rating:   userRating,
	}

	client.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func (c *ChessClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var msg map[string]interface{}
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		c.handleMessage(msg)
	}
}

func (c *ChessClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *ChessClient) handleMessage(msg map[string]interface{}) {
	msgType, _ := msg["type"].(string)

	switch msgType {
	case "join_room":
		c.handleJoinRoom(msg)
	case "move":
		c.handleMove(msg)
	case "resign":
		c.handleResign()
	case "draw_offer":
		c.handleDrawOffer()
	case "chat":
		c.handleChat(msg)
	}
}

func (c *ChessClient) handleJoinRoom(msg map[string]interface{}) {
	roomIDStr, _ := msg["room_id"].(string)
	oid, err := primitive.ObjectIDFromHex(roomIDStr)
	if err != nil {
		return
	}

	chessHub.mu.Lock()
	session, exists := chessHub.rooms[oid]
	if !exists {
		chessHub.mu.Unlock()
		return
	}

	session.mu.Lock()
	if session.Game.WhiteID == c.userID && session.WhiteConn == nil {
		session.WhiteConn = c.conn
		c.roomID = oid
	} else if session.Game.BlackID == c.userID && session.BlackConn == nil {
		session.BlackConn = c.conn
		c.roomID = oid
	} else if session.Game.WhiteID != c.userID && session.Game.BlackID != c.userID {
		session.Spectators = append(session.Spectators, c.conn)
		c.roomID = oid
	}
	session.mu.Unlock()
	chessHub.mu.Unlock()

	// ارسال state فعلی بازی
	stateMsg, _ := json.Marshal(map[string]interface{}{
		"type":      "game_state",
		"game":      session.Game,
		"room":      session.Room,
		"your_color": getPlayerColor(session.Game, c.userID),
	})
	c.send <- stateMsg

	// اطلاع به همه که کاربر join کرد
	joinMsg, _ := json.Marshal(map[string]interface{}{
		"type":     "player_joined",
		"username": c.username,
		"rating":   c.rating,
	})
	chessHub.broadcast <- BroadcastMsg{RoomID: oid, Data: joinMsg}
}

func (c *ChessClient) handleMove(msg map[string]interface{}) {
	if c.roomID.IsZero() {
		return
	}

	chessHub.mu.RLock()
	session, exists := chessHub.rooms[c.roomID]
	chessHub.mu.RUnlock()
	if !exists {
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.Game.Status != "active" {
		return
	}

	// بررسی اینکه نوبت کاربر است
	expectedColor := session.Game.Turn
	playerColor := getPlayerColor(session.Game, c.userID)
	if playerColor != expectedColor {
		errMsg, _ := json.Marshal(map[string]interface{}{
			"type": "error",
			"msg":  "Not your turn",
		})
		c.send <- errMsg
		return
	}

	from, _ := msg["from"].(string)
	to, _ := msg["to"].(string)
	promo, _ := msg["promotion"].(string)

	// در یک پیاده‌سازی واقعی، move validation باید اینجا انجام شود
	// فعلاً فقط move را ثبت می‌کنیم
	now := time.Now().UTC()
	moveNum := len(session.Game.Moves) + 1

	move := ChessMove{
		Number:    moveNum,
		From:      from,
		To:        to,
		Promotion: promo,
		SAN:       fmt.Sprintf("%s-%s", from, to),
		FEN:       session.Game.FEN,
		Timestamp: now,
	}
	session.Game.Moves = append(session.Game.Moves, move)
	session.Game.LastMoveAt = &now

	// تغییر نوبت
	if session.Game.Turn == "w" {
		session.Game.Turn = "b"
		session.Game.WhiteTime += int64(session.Game.TimeControl.IncrementS * 1000)
	} else {
		session.Game.Turn = "w"
		session.Game.BlackTime += int64(session.Game.TimeControl.IncrementS * 1000)
	}

	// ذخیره move در دیتابیس (asynchronously)
	go func(gameID primitive.ObjectID, move ChessMove) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		db.Collection("chess_games").UpdateOne(ctx,
			bson.M{"_id": gameID},
			bson.M{
				"$push": bson.M{"moves": move},
				"$set": bson.M{
					"turn":         session.Game.Turn,
					"white_time_ms": session.Game.WhiteTime,
					"black_time_ms": session.Game.BlackTime,
					"last_move_at": now,
				},
			},
		)
	}(session.Game.ID, move)

	// ارسال move به همه
	moveMsg, _ := json.Marshal(map[string]interface{}{
		"type": "move",
		"move": move,
		"turn": session.Game.Turn,
		"white_time": session.Game.WhiteTime,
		"black_time": session.Game.BlackTime,
	})
	chessHub.broadcast <- BroadcastMsg{RoomID: c.roomID, Data: moveMsg}
}

func (c *ChessClient) handleResign() {
	if c.roomID.IsZero() {
		return
	}

	chessHub.mu.RLock()
	session, exists := chessHub.rooms[c.roomID]
	chessHub.mu.RUnlock()
	if !exists {
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.Game.Status != "active" {
		return
	}

	winner := "w"
	if getPlayerColor(session.Game, c.userID) == "w" {
		winner = "b"
	}

	session.Game.Status = "resigned"
	session.Game.Winner = winner
	now := time.Now().UTC()
	session.Game.FinishedAt = &now

	go saveGame(session.Game)
	go updateRatings(session.Game)

	endMsg, _ := json.Marshal(map[string]interface{}{
		"type":   "game_over",
		"reason": "resignation",
		"winner": winner,
	})
	chessHub.broadcast <- BroadcastMsg{RoomID: c.roomID, Data: endMsg}
}

func (c *ChessClient) handleDrawOffer() {
	if c.roomID.IsZero() {
		return
	}

	drawMsg, _ := json.Marshal(map[string]interface{}{
		"type":   "draw_offered",
		"from":   c.username,
	})
	chessHub.broadcast <- BroadcastMsg{RoomID: c.roomID, Data: drawMsg}
}

func (c *ChessClient) handleChat(msg map[string]interface{}) {
	if c.roomID.IsZero() {
		return
	}

	text, _ := msg["text"].(string)
	if len(text) == 0 || len(text) > 200 {
		return
	}

	chatMsg, _ := json.Marshal(map[string]interface{}{
		"type":     "chat",
		"username": c.username,
		"text":     text,
		"time":     time.Now().UTC().Unix(),
	})
	chessHub.broadcast <- BroadcastMsg{RoomID: c.roomID, Data: chatMsg}
}

// ==================== HELPER FUNCTIONS ====================

func getPlayerColor(game *ChessGame, userID primitive.ObjectID) string {
	if game.WhiteID == userID {
		return "w"
	}
	if game.BlackID == userID {
		return "b"
	}
	return ""
}

func validateChessToken(tokenStr string) (string, primitive.ObjectID, int, error) {
	if tokenStr == "" {
		return "", primitive.NilObjectID, 0, fmt.Errorf("missing token")
	}

	// استفاده از همان JWT secret از سایت اصلی
	claims := &CustomClaims{}
	token, err := jwtParse(tokenStr, claims)
	if err != nil {
		return "", primitive.NilObjectID, 0, err
	}

	username := claims.Subject

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// فقط خواندن از users collection (دسترسی read-only)
	var user User
	err = db.Collection("users").FindOne(ctx, bson.M{"username": username}).Decode(&user)
	if err != nil {
		return "", primitive.NilObjectID, 0, fmt.Errorf("user not found")
	}

	if user.Banned {
		return "", primitive.NilObjectID, 0, fmt.Errorf("user banned")
	}

	// بررسی session salt
	if user.SessionSalt != claims.SessionSalt {
		return "", primitive.NilObjectID, 0, fmt.Errorf("session expired")
	}

	// دریافت rating شطرنج
	var rating ChessRating
	err = db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": user.ID}).Decode(&rating)
	ratingValue := 1200
	if err == nil {
		ratingValue = rating.Rating
	}

	return user.Username, user.ID, ratingValue, nil
}

// JWT parser که از همان secret استفاده می‌کند
func jwtParse(tokenStr string, claims *CustomClaims) (*jwt.Token, error) {
	return jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return cfg.JWTSecret, nil
	})
}

// ==================== GAME CLOCK ====================

func runGameClock(roomID primitive.ObjectID) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			chessHub.mu.RLock()
			session, exists := chessHub.rooms[roomID]
			chessHub.mu.RUnlock()

			if !exists {
				return
			}

			session.mu.Lock()
			if session.Game.Status != "active" {
				session.mu.Unlock()
				return
			}

			// فقط اگر هر دو بازیکن connected هستند
			if session.WhiteConn == nil || session.BlackConn == nil {
				session.mu.Unlock()
				continue
			}

			// کاهش زمان
			decrement := int64(100) // 100ms
			if session.Game.Turn == "w" {
				session.Game.WhiteTime -= decrement
				if session.Game.WhiteTime <= 0 {
					session.Game.WhiteTime = 0
					session.Game.Status = "timeout"
					session.Game.Winner = "b"
					now := time.Now().UTC()
					session.Game.FinishedAt = &now
					go saveGame(session.Game)
					go updateRatings(session.Game)

					endMsg, _ := json.Marshal(map[string]interface{}{
						"type":   "game_over",
						"reason": "timeout",
						"winner": "b",
					})
					chessHub.broadcast <- BroadcastMsg{RoomID: roomID, Data: endMsg}

					session.mu.Unlock()
					return
				}
			} else {
				session.Game.BlackTime -= decrement
				if session.Game.BlackTime <= 0 {
					session.Game.BlackTime = 0
					session.Game.Status = "timeout"
					session.Game.Winner = "w"
					now := time.Now().UTC()
					session.Game.FinishedAt = &now
					go saveGame(session.Game)
					go updateRatings(session.Game)

					endMsg, _ := json.Marshal(map[string]interface{}{
						"type":   "game_over",
						"reason": "timeout",
						"winner": "w",
					})
					chessHub.broadcast <- BroadcastMsg{RoomID: roomID, Data: endMsg}

					session.mu.Unlock()
					return
				}
			}
			session.mu.Unlock()

			// ارسال زمان به صورت دوره‌ای (هر ثانیه)
			if decrement > 0 && (session.Game.WhiteTime%1000 < 100 || session.Game.BlackTime%1000 < 100) {
				session.mu.RLock()
				timeMsg, _ := json.Marshal(map[string]interface{}{
					"type":       "clock_update",
					"white_time": session.Game.WhiteTime,
					"black_time": session.Game.BlackTime,
				})
				session.mu.RUnlock()
				chessHub.broadcast <- BroadcastMsg{RoomID: roomID, Data: timeMsg}
			}
		}
	}
}

// ==================== RATING SYSTEM (ELO) ====================

func updateRatings(game *ChessGame) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// دریافت rating های فعلی
	var whiteRating, blackRating ChessRating
	db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": game.WhiteID}).Decode(&whiteRating)
	db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": game.BlackID}).Decode(&blackRating)

	if whiteRating.Rating == 0 {
		whiteRating = ChessRating{UserID: game.WhiteID, Username: game.WhiteName, Rating: 1200}
	}
	if blackRating.Rating == 0 {
		blackRating = ChessRating{UserID: game.BlackID, Username: game.BlackName, Rating: 1200}
	}

	// محاسبه ELO
	K := 32
	expectedWhite := 1.0 / (1.0 + math.Pow(10, float64(blackRating.Rating-whiteRating.Rating)/400.0))
	expectedBlack := 1.0 - expectedWhite

	var whiteScore, blackScore float64
	switch game.Winner {
	case "w":
		whiteScore = 1.0
		blackScore = 0.0
	case "b":
		whiteScore = 0.0
		blackScore = 1.0
	case "draw":
		whiteScore = 0.5
		blackScore = 0.5
	default:
		return
	}

	newWhiteRating := int(float64(whiteRating.Rating) + float64(K)*(whiteScore-expectedWhite))
	newBlackRating := int(float64(blackRating.Rating) + float64(K)*(blackScore-expectedBlack))

	if newWhiteRating < 100 {
		newWhiteRating = 100
	}
	if newBlackRating < 100 {
		newBlackRating = 100
	}

	// بروزرسانی white
	updateRating(ctx, game.WhiteID, game.WhiteName, newWhiteRating, game.Winner == "w", game.Winner == "b", game.Winner == "draw")
	// بروزرسانی black
	updateRating(ctx, game.BlackID, game.BlackName, newBlackRating, game.Winner == "b", game.Winner == "w", game.Winner == "draw")
}

func updateRating(ctx context.Context, userID primitive.ObjectID, username string, newRating int, won, lost, drew bool) {
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			"rating":      newRating,
			"last_played": time.Now().UTC(),
			"username":    username,
		},
		"$inc": bson.M{
			"games": 1,
		},
	}

	if won {
		update["$inc"].(bson.M)["wins"] = 1
	} else if lost {
		update["$inc"].(bson.M)["losses"] = 1
	} else if drew {
		update["$inc"].(bson.M)["draws"] = 1
	}

	opts := options.Update().SetUpsert(true)
	db.Collection("chess_ratings").UpdateOne(ctx, filter, update, opts)
}

// ==================== PERSISTENCE ====================

func saveGame(game *ChessGame) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db.Collection("chess_games").UpdateOne(ctx,
		bson.M{"_id": game.ID},
		bson.M{"$set": bson.M{
			"status":      game.Status,
			"winner":      game.Winner,
			"finished_at": game.FinishedAt,
			"moves":       game.Moves,
			"white_time_ms": game.WhiteTime,
			"black_time_ms": game.BlackTime,
		}},
	)

	// پاک کردن room بعد از پایان بازی
	db.Collection("chess_rooms").DeleteOne(ctx, bson.M{"_id": game.RoomID})

	// پاک کردن session از hub
	chessHub.mu.Lock()
	delete(chessHub.rooms, game.RoomID)
	chessHub.mu.Unlock()
}

// GET /chess/leaderboard
func getLeaderboard(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options.Find().
		SetSort(bson.M{"rating": -1}).
		SetLimit(100).
		SetProjection(bson.M{"password": 0, "session_salt": 0})

	cursor, err := db.Collection("chess_ratings").Find(ctx, bson.M{}, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var ratings []ChessRating
	if err := cursor.All(ctx, &ratings); err != nil {
		c.JSON(500, gin.H{"msg": "Read error"})
		return
	}

	output := make([]gin.H, 0, len(ratings))
	for i, r := range ratings {
		output = append(output, gin.H{
			"rank":     i + 1,
			"username": r.Username,
			"rating":   r.Rating,
			"games":    r.Games,
			"wins":     r.Wins,
			"losses":   r.Losses,
			"draws":    r.Draws,
			"win_rate": func() float64 {
				if r.Games == 0 {
					return 0
				}
				return float64(r.Wins) / float64(r.Games) * 100
			}(),
		})
	}

	c.JSON(200, gin.H{
		"leaderboard": output,
		"total":       len(ratings),
	})
}

// GET /chess/games/:user_id - تاریخچه بازی‌ها
func getUserGames(c *gin.Context) {
	userIDStr := c.Param("user_id")
	oid, err := primitive.ObjectIDFromHex(userIDStr)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid user ID"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{
		"$or": []bson.M{
			{"white_id": oid},
			{"black_id": oid},
		},
	}

	opts := options.Find().
		SetSort(bson.M{"started_at": -1}).
		SetLimit(50)

	cursor, err := db.Collection("chess_games").Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var games []ChessGame
	if err := cursor.All(ctx, &games); err != nil {
		c.JSON(500, gin.H{"msg": "Read error"})
		return
	}

	output := make([]gin.H, 0, len(games))
	for _, g := range games {
		isWhite := g.WhiteID == oid
		result := "draw"
		if g.Winner == "w" {
			if isWhite {
				result = "win"
			} else {
				result = "loss"
			}
		} else if g.Winner == "b" {
			if isWhite {
				result = "loss"
			} else {
				result = "win"
			}
		}

		output = append(output, gin.H{
			"id":         g.ID.Hex(),
			"opponent":   func() string { if isWhite { return g.BlackName } else { return g.WhiteName } }(),
			"color":      func() string { if isWhite { return "white" } else { return "black" } }(),
			"result":     result,
			"status":     g.Status,
			"moves":      len(g.Moves),
			"started_at": g.StartedAt.Format(time.RFC3339),
			"duration_s": func() int {
				if g.FinishedAt != nil {
					return int(g.FinishedAt.Sub(g.StartedAt).Seconds())
				}
				return 0
			}(),
		})
	}

	c.JSON(200, gin.H{
		"games": output,
		"count": len(games),
	})
}

// ==================== MATCHMAKING ====================

type MatchmakingQueue struct {
	mu      sync.Mutex
	waiting []MatchmakingPlayer
}

type MatchmakingPlayer struct {
	UserID   primitive.ObjectID
	Username string
	Rating   int
	MaxWait  time.Duration
	JoinedAt time.Time
	Color    string // "w", "b", or "any"
	BaseMs   int
}

var matchmakingQueue = &MatchmakingQueue{
	waiting: []MatchmakingPlayer{},
}

// POST /chess/matchmaking/queue
func joinMatchmaking(c *gin.Context) {
	uVal, exists := c.Get("user")
	if !exists {
		c.JSON(401, gin.H{"msg": "Unauthorized"})
		return
	}
	user := uVal.(User)

	var body struct {
		BaseMs int    `json:"base_ms"`
		Color  string `json:"color"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		body.BaseMs = 600000
	}
	if body.Color != "w" && body.Color != "b" {
		body.Color = "any"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var rating ChessRating
	db.Collection("chess_ratings").FindOne(ctx, bson.M{"user_id": user.ID}).Decode(&rating)
	if rating.Rating == 0 {
		rating.Rating = 1200
	}

	player := MatchmakingPlayer{
		UserID:   user.ID,
		Username: user.Username,
		Rating:   rating.Rating,
		MaxWait:  2 * time.Minute,
		JoinedAt: time.Now().UTC(),
		Color:    body.Color,
		BaseMs:   body.BaseMs,
	}

	matchmakingQueue.mu.Lock()
	// حذف اگر قبلاً در صف بوده
	filtered := []MatchmakingPlayer{}
	for _, p := range matchmakingQueue.waiting {
		if p.UserID != user.ID {
			filtered = append(filtered, p)
		}
	}
	matchmakingQueue.waiting = filtered
	matchmakingQueue.waiting = append(matchmakingQueue.waiting, player)
	matchmakingQueue.mu.Unlock()

	// تلاش برای پیدا کردن حریف
	go tryMatchmaking()

	c.JSON(200, gin.H{
		"msg":          "Joined matchmaking queue",
		"queue_length": len(matchmakingQueue.waiting),
		"eta_seconds":  30,
	})
}

func tryMatchmaking() {
	matchmakingQueue.mu.Lock()
	defer matchmakingQueue.mu.Unlock()

	if len(matchmakingQueue.waiting) < 2 {
		return
	}

	// مرتب‌سازی بر اساس rating
	sort.Slice(matchmakingQueue.waiting, func(i, j int) bool {
		return matchmakingQueue.waiting[i].Rating < matchmakingQueue.waiting[j].Rating
	})

	// پیدا کردن match
	for i := 0; i < len(matchmakingQueue.waiting)-1; i++ {
		p1 := matchmakingQueue.waiting[i]
		for j := i + 1; j < len(matchmakingQueue.waiting); j++ {
			p2 := matchmakingQueue.waiting[j]

			// بررسی سازگاری
			ratingDiff := abs(p1.Rating - p2.Rating)
			if ratingDiff > 400 {
				continue
			}

			// بررسی رنگ
			if p1.Color != "any" && p2.Color != "any" && p1.Color == p2.Color {
				continue
			}

			// Match پیدا شد!
			createMatchFromQueue(p1, p2)

			// حذف از صف
			newQueue := []MatchmakingPlayer{}
			for k, p := range matchmakingQueue.waiting {
				if k != i && k != j {
					newQueue = append(newQueue, p)
				}
			}
			matchmakingQueue.waiting = newQueue
			return
		}
	}
}

func createMatchFromQueue(p1, p2 MatchmakingPlayer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// تعیین رنگ
	whitePlayer := p1
	blackPlayer := p2
	if p1.Color == "b" || p2.Color == "w" {
		whitePlayer = p2
		blackPlayer = p1
	} else if rand.Float32() < 0.5 {
		whitePlayer = p1
		blackPlayer = p2
	}

	now := time.Now().UTC()
	roomName := fmt.Sprintf("%s vs %s", whitePlayer.Username, blackPlayer.Username)

	room := ChessRoom{
		Name:       roomName,
		HostID:     whitePlayer.UserID,
		HostName:   whitePlayer.Username,
		HostRating: whitePlayer.Rating,
		HostColor:  "w",
		TimeControl: TimeControl{
			BaseMs:     p1.BaseMs,
			IncrementS: 0,
		},
		Status:      "playing",
		GuestID:     &blackPlayer.UserID,
		GuestName:   blackPlayer.Username,
		GuestRating: blackPlayer.Rating,
		CreatedAt:   now,
		StartedAt:   &now,
		ExpiresAt:   now.Add(1 * time.Hour),
	}

	res, err := db.Collection("chess_rooms").InsertOne(ctx, room)
	if err != nil {
		return
	}
	room.ID = res.InsertedID.(primitive.ObjectID)

	game := ChessGame{
		RoomID:    room.ID,
		WhiteID:   whitePlayer.UserID,
		WhiteName: whitePlayer.Username,
		BlackID:   blackPlayer.UserID,
		BlackName: blackPlayer.Username,
		FEN:       StartFEN,
		Turn:      "w",
		Moves:     []ChessMove{},
		Status:    "active",
		WhiteTime: int64(room.TimeControl.BaseMs),
		BlackTime: int64(room.TimeControl.BaseMs),
		StartedAt: now,
		TimeControl: room.TimeControl,
	}

	res, err = db.Collection("chess_games").InsertOne(ctx, game)
	if err != nil {
		return
	}
	game.ID = res.InsertedID.(primitive.ObjectID)

	session := &RoomSession{
		Room:      &room,
		Game:      &game,
		ClockTick: make(chan struct{}),
	}

	chessHub.mu.Lock()
	chessHub.rooms[room.ID] = session
	chessHub.mu.Unlock()

	go runGameClock(room.ID)

	log.Printf("Matchmaking: Created match %s (%d) vs %s (%d)",
		whitePlayer.Username, whitePlayer.Rating,
		blackPlayer.Username, blackPlayer.Rating)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// ==================== CLEANUP ====================

func cleanupChessTasks() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Chess Cleanup Panic: %v", r)
		}
	}()

	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-shutdownCh:
			return
		case <-ticker.C:
			if db != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

				// پاک کردن روم‌های منقضی شده
				db.Collection("chess_rooms").DeleteMany(ctx, bson.M{
					"status":     "waiting",
					"expires_at": bson.M{"$lt": time.Now().UTC()},
				})

				// پیدا کردن بازی‌های orphaned (بدون بازیکن متصل)
				// این می‌تواند بر اساس نیاز گسترش یابد

				cancel()
			}
		}
	}
}

// ==================== ROUTE SETUP ====================

func SetupChessRoutes(r *gin.Engine) {
	// راه‌اندازی WebSocket hub
	go chessHub.Run()
	go cleanupChessTasks()

	// مسیرهای عمومی شطرنج
	chess := r.Group("/chess")
	{
		chess.GET("/leaderboard", getLeaderboard)
		chess.GET("/rooms", listChessRooms)
		chess.GET("/stats", RateLimitMiddleware(rlGeneral, func(c *gin.Context) string {
			return tokenKeyHash("chess_stats_", c)
		}), AuthMiddleware(false), getChessStats)

		// روم‌ها (نیاز به احراز هویت)
		chessAuth := chess.Group("")
		chessAuth.Use(AuthMiddleware(false))
		{
			chessAuth.POST("/rooms", createChessRoom)
			chessAuth.POST("/rooms/:room_id/join", joinChessRoom)
			chessAuth.GET("/games/:user_id", getUserGames)
			chessAuth.POST("/matchmaking/queue", joinMatchmaking)
		}

		// WebSocket (بدون middleware - احراز هویت از طریق token)
		chess.GET("/ws", chessWebSocket)
	}

	// ایجاد index ها برای کلکسیون‌های شطرنج
	go ensureChessIndexes()
}

func ensureChessIndexes() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if db == nil {
		return
	}

	db.Collection("chess_ratings").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "user_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})

	db.Collection("chess_ratings").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "rating", Value: -1}},
	})

	db.Collection("chess_rooms").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}, {Key: "expires_at", Value: 1}},
	})

	db.Collection("chess_rooms").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "host_id", Value: 1}},
	})

	db.Collection("chess_games").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "room_id", Value: 1}},
	})

	db.Collection("chess_games").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "white_id", Value: 1}, {Key: "started_at", Value: -1}},
	})

	db.Collection("chess_games").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "black_id", Value: 1}, {Key: "started_at", Value: -1}},
	})

	db.Collection("chess_games").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}},
	})

	log.Println("Chess indexes created")
}
