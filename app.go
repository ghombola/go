package main

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

// ==================== CONFIG ====================

type Config struct {
	MongoURI       string
	DBName         string
	JWTSecret      []byte
	Port           string
	WorkerCount    int
	JobWaitSeconds time.Duration
	AdminUsernames []string
	AdminEnvUser   string
	AdminEnvPass   string
}

var cfg Config
var mongoClient *mongo.Client
var db *mongo.Database

func loadConfig() {
	_ = godotenv.Load()

	cfg.MongoURI = os.Getenv("MONGO_URI")
	if cfg.MongoURI == "" {
		log.Println("WARNING: MONGO_URI is missing")
	}
	cfg.JWTSecret = []byte(os.Getenv("JWT_SECRET_KEY"))
	if len(cfg.JWTSecret) == 0 {
		log.Println("WARNING: JWT_SECRET_KEY is missing, generic fallback used (NOT SAFE FOR PROD)")
		cfg.JWTSecret = []byte("default-insecure-secret")
	}

	cfg.DBName = os.Getenv("MONGO_DBNAME")
	if cfg.DBName == "" {
		cfg.DBName = "test"
	}

	cfg.Port = os.Getenv("PORT")
	if cfg.Port == "" {
		cfg.Port = "5001"
	}

	wc, _ := strconv.Atoi(os.Getenv("WORKER_COUNT"))
	if wc <= 0 {
		wc = 4
	}
	cfg.WorkerCount = wc

	ws, _ := strconv.ParseFloat(os.Getenv("JOB_WAIT_SECONDS"), 64)
	if ws == 0 {
		ws = 8.0
	}
	cfg.JobWaitSeconds = time.Duration(ws * float64(time.Second))

	admins := os.Getenv("ADMIN_USERNAMES")
	cfg.AdminUsernames = []string{}
	for _, u := range strings.Split(admins, ",") {
		if t := strings.TrimSpace(u); t != "" {
			cfg.AdminUsernames = append(cfg.AdminUsernames, strings.ToLower(t))
		}
	}
	cfg.AdminEnvUser = strings.ToLower(os.Getenv("ADMIN_USERNAME"))
	cfg.AdminEnvPass = os.Getenv("ADMIN_PASSWORD")
}

// ==================== DATABASE ====================

func connectDB() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if cfg.MongoURI == "" {
		log.Fatal("Fatal: MongoURI is empty. Cannot connect to database.")
	}

	clientOptions := options.Client().ApplyURI(cfg.MongoURI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to create Mongo client: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("MongoDB Ping Failed (Check network/credentials): %v", err)
	}

	mongoClient = client
	db = client.Database(cfg.DBName)
	log.Printf("Connected to MongoDB: %s", cfg.DBName)
}

// ==================== MODELS ====================

type User struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Username       string             `bson:"username" json:"username"`
	Password       string             `bson:"password" json:"-"`
	Role           string             `bson:"role" json:"role"`
	SessionSalt    string             `bson:"session_salt" json:"-"`
	CreatedAt      time.Time          `bson:"created_at" json:"created_at"`
	TotalPurchases int                `bson:"total_purchases" json:"total_purchases"`
	ExpiryDate     *time.Time         `bson:"expiryDate,omitempty" json:"expiryDate,omitempty"`
	// --- NEW FIELDS (backward compatible: omitempty + default) ---
	Banned     bool      `bson:"banned,omitempty" json:"banned,omitempty"`
	BannedAt   *time.Time `bson:"banned_at,omitempty" json:"banned_at,omitempty"`
	BannedBy   string    `bson:"banned_by,omitempty" json:"banned_by,omitempty"`
	BanReason  string    `bson:"ban_reason,omitempty" json:"ban_reason,omitempty"`
	Coins      int64     `bson:"coins,omitempty" json:"coins,omitempty"`
}

type Coupon struct {
	ID        primitive.ObjectID   `bson:"_id,omitempty" json:"id"`
	Code      string               `bson:"code" json:"code"`
	BonusDays int                  `bson:"bonus_days" json:"bonus_days"`
	MaxUses   *int                 `bson:"max_uses" json:"max_uses"`
	Uses      int                  `bson:"uses" json:"uses"`
	UsedBy    []primitive.ObjectID `bson:"used_by" json:"used_by"`
	ExpiresAt *time.Time           `bson:"expires_at" json:"expires_at"`
	CreatedAt time.Time            `bson:"created_at" json:"created_at"`
}

type Transaction struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID       primitive.ObjectID `bson:"user_id" json:"user_id"`
	Username     string             `bson:"username" json:"username"`
	TxHash       string             `bson:"tx_hash" json:"tx_hash"`
	Days         int                `bson:"days" json:"days"`
	Amount       int64              `bson:"amount,omitempty" json:"amount,omitempty"`
	PlanName     string             `bson:"plan_name,omitempty" json:"plan_name,omitempty"`
	Type         string             `bson:"type" json:"type"` // "subscription", "coin_purchase", "donation"
	Status       string             `bson:"status" json:"status"`
	CreatedAt    time.Time          `bson:"created_at" json:"created_at"`
	ProcessedAt  *time.Time         `bson:"processed_at,omitempty" json:"processed_at,omitempty"`
	ApprovedBy   string             `bson:"approved_by,omitempty" json:"approved_by,omitempty"`
	RejectedAt   *time.Time         `bson:"rejected_at,omitempty" json:"rejected_at,omitempty"`
	RejectedBy   string             `bson:"rejected_by,omitempty" json:"rejected_by,omitempty"`
	RejectReason string             `bson:"reject_reason,omitempty" json:"reject_reason,omitempty"`
}

type PaymentOrder struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID    primitive.ObjectID `bson:"user_id" json:"user_id"`
	Plan      string             `bson:"plan" json:"plan"`
	Amount    int64              `bson:"amount" json:"amount"`
	Status    string             `bson:"status" json:"status"`
	TrackID   string             `bson:"track_id,omitempty" json:"track_id,omitempty"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
	PaidAt    *time.Time         `bson:"paid_at,omitempty" json:"paid_at,omitempty"`
	// --- NEW ---
	Type      string `bson:"type,omitempty" json:"type,omitempty"` // "subscription", "coins"
	CoinAmount int64  `bson:"coin_amount,omitempty" json:"coin_amount,omitempty"`
}

// Plan - moved to DB for dynamic management
type Plan struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name        string             `bson:"name" json:"name"`
	DisplayName string             `bson:"display_name" json:"display_name"`
	Days        int                `bson:"days" json:"days"`
	Amount      int64              `bson:"amount" json:"amount"` // ریال
	Active      bool               `bson:"active" json:"active"`
	SortOrder   int                `bson:"sort_order" json:"sort_order"`
	CreatedAt   time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt   time.Time          `bson:"updated_at" json:"updated_at"`
	// Discount
	DiscountPercent int        `bson:"discount_percent,omitempty" json:"discount_percent,omitempty"`
	DiscountUntil   *time.Time `bson:"discount_until,omitempty" json:"discount_until,omitempty"`
	// Quantity discount: e.g. buy 3+ get 10% off
	QuantityDiscounts []QuantityDiscount `bson:"quantity_discounts,omitempty" json:"quantity_discounts,omitempty"`
}

type QuantityDiscount struct {
	MinQty          int `bson:"min_qty" json:"min_qty"`
	DiscountPercent int `bson:"discount_percent" json:"discount_percent"`
}

// CoinPackage - for coin purchases
type CoinPackage struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name      string             `bson:"name" json:"name"`
	Coins     int64              `bson:"coins" json:"coins"`
	Amount    int64              `bson:"amount" json:"amount"` // ریال
	Active    bool               `bson:"active" json:"active"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
}

// ==================== LEGACY PLANS (fallback) ====================
// These are used ONLY if DB plans collection is empty (backward compat)
var LegacyPlans = map[string]struct {
	Days   int
	Amount int64
}{
	"starter": {Days: 30, Amount: 760000},
	"pro":     {Days: 90, Amount: 2290000},
	"elite":   {Days: 180, Amount: 4290000},
	"ultra":   {Days: 365, Amount: 8290000},
}

func getPlanByName(ctx context.Context, name string) (*Plan, error) {
	var plan Plan
	err := db.Collection("plans").FindOne(ctx, bson.M{"name": name, "active": true}).Decode(&plan)
	if err == mongo.ErrNoDocuments {
		// Fallback to legacy hardcoded plans
		if lp, ok := LegacyPlans[name]; ok {
			return &Plan{
				Name:   name,
				Days:   lp.Days,
				Amount: lp.Amount,
				Active: true,
			}, nil
		}
		return nil, fmt.Errorf("plan not found: %s", name)
	}
	return &plan, err
}

func getEffectiveAmount(plan *Plan) int64 {
	amount := plan.Amount
	now := time.Now().UTC()

	// Apply time-based discount
	if plan.DiscountPercent > 0 && plan.DiscountUntil != nil && plan.DiscountUntil.After(now) {
		amount = amount * int64(100-plan.DiscountPercent) / 100
	}
	return amount
}

// ==================== RATE LIMITER ====================

type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	rate     float64 // tokens per second
	capacity float64 // max burst
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

func NewRateLimiter(ratePerMinute float64, burst float64) *RateLimiter {
	return &RateLimiter{
		buckets:  make(map[string]*tokenBucket),
		rate:     ratePerMinute / 60.0,
		capacity: burst,
	}
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, exists := rl.buckets[key]
	if !exists {
		rl.buckets[key] = &tokenBucket{tokens: rl.capacity - 1, lastRefill: now}
		return true
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > rl.capacity {
		b.tokens = rl.capacity
	}
	b.lastRefill = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Cleanup old entries periodically
func (rl *RateLimiter) Cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-shutdownCh:
				ticker.Stop()
				return
			case <-ticker.C:
				rl.mu.Lock()
				now := time.Now()
				for k, b := range rl.buckets {
					if now.Sub(b.lastRefill) > 5*time.Minute {
						delete(rl.buckets, k)
					}
				}
				rl.mu.Unlock()
			}
		}
	}()
}

// Rate limiters with different configs
var (
	// /auth/me - very generous: 150/min per user, burst 200
	rlAuthMe = NewRateLimiter(150, 200)
	// /auth/login, /auth/register - strict: 10/min per IP, burst 15
	rlAuth = NewRateLimiter(10, 15)
	// General authenticated endpoints: 60/min per user
	rlGeneral = NewRateLimiter(60, 80)
	// Admin endpoints: 120/min per admin
	rlAdmin = NewRateLimiter(120, 150)
	// Payment creation: 10/min per user
	rlPayment = NewRateLimiter(10, 15)
)

func RateLimitMiddleware(limiter *RateLimiter, keyFunc func(*gin.Context) string) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := keyFunc(c)
		if !limiter.Allow(key) {
			c.AbortWithStatusJSON(429, gin.H{
				"msg":         "Too many requests",
				"retry_after": 60,
			})
			return
		}
		c.Next()
	}
}

// ==================== UTILS ====================

func hashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	return string(bytes), err
}

func checkPassword(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(t)
	}
}

func interfaceToInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case float64:
		return int64(t), true
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
		return 0, false
	case string:
		if i, err := strconv.ParseInt(t, 10, 64); err == nil {
			return i, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func getClientIP(c *gin.Context) string {
	// Check X-Forwarded-For first (behind proxy)
	if xff := c.GetHeader("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	if xri := c.GetHeader("X-Real-Ip"); xri != "" {
		return xri
	}
	return c.ClientIP()
}

// ==================== WORKER ENGINE ====================

type JobResult struct {
	Data interface{}
	Err  error
	Code int
}

type Job struct {
	Priority   int
	Sequence   int64
	Func       func() (interface{}, int, error)
	ResultChan chan JobResult
}

type PriorityQueue []*Job

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	return pq[i].Sequence < pq[j].Sequence
}
func (pq PriorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *PriorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*Job)) }
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

var (
	jobQueue   = make(PriorityQueue, 0)
	queueLock  sync.Mutex
	jobSignal  = make(chan struct{}, 1000)
	shutdownCh = make(chan struct{})
)

func safeExecWrap(task func() (interface{}, int, error)) (data interface{}, code int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("PANIC in worker: %v", r)
			code = 500
			log.Printf("[CRITICAL] Worker Panic: %v", r)
		}
	}()
	return task()
}

func SubmitJob(priority int, task func() (interface{}, int, error), wait bool) (interface{}, int, error) {
	resChan := make(chan JobResult, 1)
	job := &Job{
		Priority:   priority,
		Sequence:   time.Now().UnixNano(),
		Func:       task,
		ResultChan: resChan,
	}

	queueLock.Lock()
	heap.Push(&jobQueue, job)
	queueLock.Unlock()

	select {
	case jobSignal <- struct{}{}:
	default:
	}

	if !wait {
		return map[string]interface{}{"queued": true, "job_id": job.Sequence}, 202, nil
	}

	select {
	case res := <-resChan:
		return res.Data, res.Code, res.Err
	case <-time.After(cfg.JobWaitSeconds):
		return map[string]string{"msg": "Processing queued due to load (timeout)"}, 202, nil
	}
}

func startWorkers(count int) {
	for i := 0; i < count; i++ {
		go func(id int) {
			log.Printf("Worker-%d started", id)
			for {
				select {
				case <-shutdownCh:
					return
				case <-jobSignal:
					queueLock.Lock()
					if jobQueue.Len() == 0 {
						queueLock.Unlock()
						continue
					}
					rawItem := heap.Pop(&jobQueue)
					queueLock.Unlock()

					if rawItem == nil {
						continue
					}
					item := rawItem.(*Job)

					data, code, err := safeExecWrap(item.Func)

					if err != nil && data == nil {
						data = map[string]string{"msg": "Processing Error", "detail": err.Error()}
						if code == 0 {
							code = 500
						}
					}

					select {
					case item.ResultChan <- JobResult{Data: data, Code: code, Err: err}:
					default:
					}
					close(item.ResultChan)
				}
			}
		}(i)
	}
}

// ==================== BUSINESS LOGIC ====================

func logicApplyPayment(userIDStr string, couponCode, txHash string, daysReq int) (interface{}, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	userOID, err := primitive.ObjectIDFromHex(userIDStr)
	if err != nil {
		return map[string]string{"msg": "Invalid user ID"}, 400, nil
	}

	if db == nil {
		return nil, 500, fmt.Errorf("Database connection lost")
	}

	var user User
	err = db.Collection("users").FindOne(ctx, bson.M{"_id": userOID}).Decode(&user)
	if err != nil {
		return map[string]string{"msg": "User not found"}, 404, err
	}

	// Check if user is banned
	if user.Banned {
		return map[string]string{"msg": "Account is suspended"}, 403, nil
	}

	if couponCode != "" {
		couponColl := db.Collection("coupons")
		var cp Coupon

		err := couponColl.FindOne(ctx, bson.M{"code": couponCode}).Decode(&cp)
		if err != nil {
			return map[string]string{"msg": "Invalid coupon code"}, 400, nil
		}

		now := time.Now().UTC()
		if cp.ExpiresAt != nil && cp.ExpiresAt.Before(now) {
			couponColl.DeleteOne(ctx, bson.M{"_id": cp.ID})
			return map[string]string{"msg": "Coupon expired"}, 400, nil
		}
		if cp.MaxUses != nil && cp.Uses >= *cp.MaxUses {
			couponColl.DeleteOne(ctx, bson.M{"_id": cp.ID})
			return map[string]string{"msg": "Coupon limits reached"}, 400, nil
		}

		if cp.UsedBy == nil {
			cp.UsedBy = []primitive.ObjectID{}
		}

		for _, uID := range cp.UsedBy {
			if uID == userOID {
				return map[string]string{"msg": "You have already used this coupon"}, 400, nil
			}
		}

		filter := bson.M{
			"_id":     cp.ID,
			"uses":    cp.Uses,
			"used_by": bson.M{"$ne": userOID},
		}
		update := bson.M{
			"$inc":  bson.M{"uses": 1},
			"$push": bson.M{"used_by": userOID},
		}

		var updatedCp Coupon
		err = couponColl.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&updatedCp)
		if err != nil {
			return map[string]string{"msg": "Coupon unavailable or usage conflict"}, 409, nil
		}

		startPoint := time.Now().UTC()
		if user.ExpiryDate != nil && user.ExpiryDate.After(startPoint) {
			startPoint = *user.ExpiryDate
		}
		newExpiry := startPoint.Add(time.Duration(cp.BonusDays) * 24 * time.Hour)

		_, err = db.Collection("users").UpdateOne(ctx, bson.M{"_id": userOID}, bson.M{
			"$set": bson.M{"expiryDate": newExpiry},
			"$inc": bson.M{"total_purchases": 1},
		})

		if err != nil {
			couponColl.UpdateOne(context.Background(), bson.M{"_id": cp.ID}, bson.M{
				"$inc":  bson.M{"uses": -1},
				"$pull": bson.M{"used_by": userOID},
			})
			return map[string]string{"msg": "Failed to apply bonus"}, 500, err
		}

		if updatedCp.MaxUses != nil && updatedCp.Uses >= *updatedCp.MaxUses {
			couponColl.DeleteOne(context.Background(), bson.M{"_id": updatedCp.ID})
		}

		return map[string]interface{}{
			"msg":        "Coupon applied",
			"new_expiry": newExpiry.Format(time.RFC3339),
		}, 200, nil
	}

	if txHash == "" {
		return map[string]string{"msg": "TX Hash required"}, 400, nil
	}

	count, _ := db.Collection("transactions").CountDocuments(ctx, bson.M{"tx_hash": txHash})
	if count > 0 {
		return map[string]string{"msg": "Transaction exists"}, 409, nil
	}

	newTx := Transaction{
		UserID:    userOID,
		Username:  user.Username,
		TxHash:    txHash,
		Days:      daysReq,
		Type:      "subscription",
		Status:    "pending",
		CreatedAt: time.Now().UTC(),
	}

	res, err := db.Collection("transactions").InsertOne(ctx, newTx)
	if err != nil {
		return nil, 500, err
	}

	idStr := "unknown"
	if oid, ok := res.InsertedID.(primitive.ObjectID); ok {
		idStr = oid.Hex()
	}

	return map[string]interface{}{
		"msg":   "Pending approval",
		"tx_id": idStr,
	}, 201, nil
}

// ==================== AUTH & MIDDLEWARE ====================

type CustomClaims struct {
	SessionSalt string `json:"session_salt"`
	jwt.RegisteredClaims
}

func generateTokens(username, salt string) (string, string, error) {
	accClaims := CustomClaims{
		SessionSalt: salt,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(2160 * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, accClaims)
	accStr, err := token.SignedString(cfg.JWTSecret)
	if err != nil {
		return "", "", err
	}

	refClaims := CustomClaims{
		SessionSalt: salt,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   username,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(2160 * time.Hour)),
		},
	}
	rToken := jwt.NewWithClaims(jwt.SigningMethodHS256, refClaims)
	refStr, err := rToken.SignedString(cfg.JWTSecret)
	return accStr, refStr, err
}

func AuthMiddleware(requiredAdmin bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(401, gin.H{"msg": "Missing Token"})
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		claims := &CustomClaims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return cfg.JWTSecret, nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(401, gin.H{"msg": "Invalid Token"})
			return
		}

		username := claims.Subject
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var user User
		err = db.Collection("users").FindOne(ctx, bson.M{"username": username}).Decode(&user)

		if err != nil || user.SessionSalt != claims.SessionSalt {
			c.AbortWithStatusJSON(401, gin.H{"msg": "Session Expired"})
			return
		}

		// Check ban status
		if user.Banned {
			c.AbortWithStatusJSON(403, gin.H{
				"msg":        "Account suspended",
				"reason":     user.BanReason,
				"banned_at":  user.BannedAt,
			})
			return
		}

		isAdmin := user.Role == "admin"
		for _, adm := range cfg.AdminUsernames {
			if adm == username {
				isAdmin = true
				break
			}
		}
		if cfg.AdminEnvUser != "" && username == cfg.AdminEnvUser {
			isAdmin = true
		}

		if requiredAdmin && !isAdmin {
			c.AbortWithStatusJSON(403, gin.H{"msg": "Forbidden"})
			return
		}

		c.Set("user", user)
		c.Set("is_admin", isAdmin)
		c.Next()
	}
}

// ==================== HANDLERS ====================

func register(c *gin.Context) {
	var body struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Missing fields"})
		return
	}

	username := strings.ToLower(strings.TrimSpace(body.Username))
	if len(username) < 3 {
		c.JSON(400, gin.H{"msg": "Username too short"})
		return
	}
	if len(username) > 32 {
		c.JSON(400, gin.H{"msg": "Username too long (max 32)"})
		return
	}
	// Validate username characters
	for _, ch := range username {
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_') {
			c.JSON(400, gin.H{"msg": "Username can only contain lowercase letters, numbers, and underscores"})
			return
		}
	}

	if len(body.Password) < 6 {
		c.JSON(400, gin.H{"msg": "Password too short (min 6)"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	count, _ := db.Collection("users").CountDocuments(ctx, bson.M{"username": username})
	if count > 0 {
		c.JSON(409, gin.H{"msg": "Username exists"})
		return
	}

	hashed, err := hashPassword(body.Password)
	if err != nil {
		c.JSON(500, gin.H{"msg": "Server error"})
		return
	}
	salt := strconv.FormatInt(time.Now().UnixNano(), 10)

	role := "user"
	for _, adm := range cfg.AdminUsernames {
		if adm == username {
			role = "admin"
		}
	}
	if cfg.AdminEnvUser == username {
		role = "admin"
	}

	newUser := User{
		Username:    username,
		Password:    hashed,
		Role:        role,
		SessionSalt: salt,
		CreatedAt:   time.Now().UTC(),
		Coins:       0,
	}

	_, err = db.Collection("users").InsertOne(ctx, newUser)
	if err != nil {
		c.JSON(500, gin.H{"msg": "Registration failed"})
		return
	}

	at, rt, _ := generateTokens(username, salt)
	c.JSON(201, gin.H{"msg": "Registered", "access_token": at, "refresh_token": rt})
}

func login(c *gin.Context) {
	var body struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	// BUG FIX: properly handle bind error
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Invalid request body"})
		return
	}

	username := strings.ToLower(strings.TrimSpace(body.Username))
	if username == "" {
		c.JSON(400, gin.H{"msg": "Username required"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user User
	err := db.Collection("users").FindOne(ctx, bson.M{"username": username}).Decode(&user)
	if err != nil || !checkPassword(body.Password, user.Password) {
		c.JSON(401, gin.H{"msg": "Invalid credentials"})
		return
	}

	// Check ban
	if user.Banned {
		c.JSON(403, gin.H{
			"msg":       "Account suspended",
			"reason":    user.BanReason,
			"banned_at": user.BannedAt,
		})
		return
	}

	newSalt := strconv.FormatInt(time.Now().UnixNano(), 10)
	db.Collection("users").UpdateOne(ctx, bson.M{"_id": user.ID}, bson.M{"$set": bson.M{"session_salt": newSalt}})

	at, rt, _ := generateTokens(username, newSalt)
	c.JSON(200, gin.H{"access_token": at, "refresh_token": rt})
}

// BUG FIX: getMe no longer goes through worker queue - direct DB read for performance
func getMe(c *gin.Context) {
	uVal, exists := c.Get("user")
	if !exists {
		c.JSON(401, gin.H{"msg": "Unauthorized context"})
		return
	}

	currentUser, ok := uVal.(User)
	if !ok {
		c.JSON(500, gin.H{"msg": "User context corrupted"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user User
	err := db.Collection("users").FindOne(ctx, bson.M{"_id": currentUser.ID}).Decode(&user)
	if err != nil {
		c.JSON(404, gin.H{"msg": "User not found"})
		return
	}

	now := time.Now().UTC()
	daysLeft := 0
	var expIso interface{} = nil
	isActive := false

	if user.ExpiryDate != nil {
		iso := user.ExpiryDate.Format(time.RFC3339)
		expIso = iso
		if user.ExpiryDate.After(now) {
			diff := user.ExpiryDate.Sub(now)
			daysLeft = int(math.Ceil(diff.Hours() / 24))
			isActive = true
		}
	}

	c.JSON(200, gin.H{
		"username":   user.Username,
		"role":       user.Role,
		"days_left":  daysLeft,
		"expiry_iso": expIso,
		"is_active":  isActive,
		"coins":      user.Coins,
		"created_at": user.CreatedAt.Format(time.RFC3339),
	})
}

func submitPayment(c *gin.Context) {
	uVal, exists := c.Get("user")
	if !exists {
		c.JSON(401, gin.H{"msg": "Unauthorized"})
		return
	}
	currentUser := uVal.(User)

	var body struct {
		Days       *int   `json:"days"`
		TxHash     string `json:"tx_hash"`
		CouponCode string `json:"coupon_code"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Invalid JSON"})
		return
	}

	days := 0
	if body.Days != nil {
		days = *body.Days
	}

	if body.CouponCode == "" && (days < 1 || days > 3650) {
		c.JSON(400, gin.H{"msg": "Days required (1-3650) if no coupon"})
		return
	}

	task := func() (interface{}, int, error) {
		return logicApplyPayment(currentUser.ID.Hex(), body.CouponCode, body.TxHash, days)
	}

	res, code, err := SubmitJob(10, task, true)
	if err != nil {
		if resMap, ok := res.(map[string]string); ok {
			c.JSON(code, resMap)
			return
		}
		c.JSON(code, gin.H{"msg": "System failure", "error": err.Error()})
		return
	}
	c.JSON(code, res)
}

// ==================== USER TRANSACTION HISTORY ====================

func getUserTransactions(c *gin.Context) {
	uVal, _ := c.Get("user")
	user := uVal.(User)

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}
	skip := int64((page - 1) * limit)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"user_id": user.ID}

	// Optional status filter
	if status := c.Query("status"); status != "" {
		filter["status"] = status
	}

	total, _ := db.Collection("transactions").CountDocuments(ctx, filter)

	opts := options.Find().
		SetSort(bson.M{"created_at": -1}).
		SetSkip(skip).
		SetLimit(int64(limit))

	cursor, err := db.Collection("transactions").Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var txs []Transaction
	if err := cursor.All(ctx, &txs); err != nil {
		c.JSON(500, gin.H{"msg": "Read error"})
		return
	}

	output := make([]gin.H, 0, len(txs))
	for _, tx := range txs {
		item := gin.H{
			"id":         tx.ID.Hex(),
			"tx_hash":    tx.TxHash,
			"days":       tx.Days,
			"type":       tx.Type,
			"status":     tx.Status,
			"created_at": tx.CreatedAt.Format(time.RFC3339),
		}
		if tx.Amount > 0 {
			item["amount"] = tx.Amount
		}
		if tx.PlanName != "" {
			item["plan"] = tx.PlanName
		}
		if tx.ProcessedAt != nil {
			item["processed_at"] = tx.ProcessedAt.Format(time.RFC3339)
		}
		if tx.RejectReason != "" {
			item["reject_reason"] = tx.RejectReason
		}
		output = append(output, item)
	}

	c.JSON(200, gin.H{
		"transactions": output,
		"total":        total,
		"page":         page,
		"limit":        limit,
		"pages":        int(math.Ceil(float64(total) / float64(limit))),
	})
}

// ==================== ADMIN HANDLERS ====================

func adminListTx(c *gin.Context) {
	status := c.Query("status")
	txType := c.Query("type")
	username := c.Query("username")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 200 {
		limit = 50
	}

	filter := bson.M{}
	if status != "" {
		filter["status"] = status
	}
	if txType != "" {
		filter["type"] = txType
	}
	if username != "" {
		filter["username"] = bson.M{"$regex": username, "$options": "i"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	total, _ := db.Collection("transactions").CountDocuments(ctx, filter)

	skip := int64((page - 1) * limit)
	opts := options.Find().SetSort(bson.M{"created_at": -1}).SetSkip(skip).SetLimit(int64(limit))
	cursor, err := db.Collection("transactions").Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var rawResults []Transaction
	if err := cursor.All(ctx, &rawResults); err != nil {
		c.JSON(500, gin.H{"msg": "Cursor read error"})
		return
	}

	output := make([]gin.H, 0, len(rawResults))
	for _, tx := range rawResults {
		item := gin.H{
			"_id":        tx.ID.Hex(),
			"user_id":    tx.UserID.Hex(),
			"username":   tx.Username,
			"tx_hash":    tx.TxHash,
			"days":       tx.Days,
			"type":       tx.Type,
			"status":     tx.Status,
			"created_at": tx.CreatedAt.Format(time.RFC3339),
		}
		if tx.Amount > 0 {
			item["amount"] = tx.Amount
		}
		if tx.PlanName != "" {
			item["plan"] = tx.PlanName
		}
		if tx.ProcessedAt != nil {
			item["processed_at"] = tx.ProcessedAt.Format(time.RFC3339)
		}
		if tx.ApprovedBy != "" {
			item["approved_by"] = tx.ApprovedBy
		}
		if tx.RejectedAt != nil {
			item["rejected_at"] = tx.RejectedAt.Format(time.RFC3339)
		}
		if tx.RejectedBy != "" {
			item["rejected_by"] = tx.RejectedBy
		}
		if tx.RejectReason != "" {
			item["reject_reason"] = tx.RejectReason
		}
		output = append(output, item)
	}
	c.JSON(200, gin.H{
		"transactions": output,
		"total":        total,
		"page":         page,
		"limit":        limit,
	})
}

func adminApproveTx(c *gin.Context) {
	txID := c.Param("tx_id")
	uVal, _ := c.Get("user")
	admin, ok := uVal.(User)
	if !ok {
		c.JSON(401, gin.H{"msg": "Auth Error"})
		return
	}

	task := func() (interface{}, int, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		oid, err := primitive.ObjectIDFromHex(txID)
		if err != nil {
			return map[string]string{"msg": "Invalid ID"}, 400, nil
		}

		var tx Transaction
		if err := db.Collection("transactions").FindOne(ctx, bson.M{"_id": oid, "status": "pending"}).Decode(&tx); err != nil {
			return map[string]string{"msg": "TX not pending"}, 404, nil
		}

		var targetUser User
		if err := db.Collection("users").FindOne(ctx, bson.M{"_id": tx.UserID}).Decode(&targetUser); err != nil {
			return map[string]string{"msg": "User missing"}, 404, nil
		}

		now := time.Now().UTC()
		start := now
		if targetUser.ExpiryDate != nil && targetUser.ExpiryDate.After(now) {
			start = *targetUser.ExpiryDate
		}
		newExp := start.Add(time.Duration(tx.Days) * 24 * time.Hour)

		_, err = db.Collection("users").UpdateOne(ctx, bson.M{"_id": targetUser.ID}, bson.M{
			"$set": bson.M{"expiryDate": newExp},
			"$inc": bson.M{"total_purchases": 1},
		})

		if err == nil {
			db.Collection("transactions").UpdateOne(ctx, bson.M{"_id": oid}, bson.M{
				"$set": bson.M{
					"status":       "approved",
					"approved_by":  admin.Username,
					"processed_at": now,
				},
			})
		}

		return map[string]string{"msg": "Approved", "new_expiry": newExp.Format(time.RFC3339)}, 200, nil
	}

	res, code, _ := SubmitJob(5, task, true)
	c.JSON(code, res)
}

func adminRejectTx(c *gin.Context) {
	txID := c.Param("tx_id")
	uVal, _ := c.Get("user")
	admin := uVal.(User)

	// BUG FIX: proper struct definition
	var body struct {
		Reason string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		body.Reason = ""
	}
	if body.Reason == "" {
		body.Reason = "No reason"
	}

	task := func() (interface{}, int, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		oid, err := primitive.ObjectIDFromHex(txID)
		if err != nil {
			return map[string]string{"msg": "ID error"}, 400, nil
		}

		res, err := db.Collection("transactions").UpdateOne(ctx,
			bson.M{"_id": oid, "status": "pending"},
			bson.M{"$set": bson.M{
				"status":        "rejected",
				"rejected_by":   admin.Username,
				"rejected_at":   time.Now().UTC(),
				"reject_reason": body.Reason,
			}},
		)
		if err != nil || res.MatchedCount == 0 {
			return map[string]string{"msg": "Update failed (not found/processed)"}, 404, nil
		}
		return map[string]string{"msg": "Rejected"}, 200, nil
	}

	res, code, _ := SubmitJob(5, task, true)
	c.JSON(code, res)
}

func manageCoupons(c *gin.Context) {
	if c.Request.Method == "GET" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cursor, err := db.Collection("coupons").Find(ctx, bson.M{}, options.Find().SetSort(bson.M{"created_at": -1}))
		if err != nil {
			c.JSON(500, gin.H{"msg": "DB error"})
			return
		}
		defer cursor.Close(ctx)

		var coupons []Coupon
		// BUG FIX: check cursor.All error
		if err := cursor.All(ctx, &coupons); err != nil {
			c.JSON(500, gin.H{"msg": "Read error"})
			return
		}
		if coupons == nil {
			coupons = []Coupon{}
		}

		output := make([]gin.H, 0, len(coupons))
		for _, cp := range coupons {
			usedByStrs := make([]string, 0, len(cp.UsedBy))
			for _, u := range cp.UsedBy {
				usedByStrs = append(usedByStrs, u.Hex())
			}

			item := gin.H{
				"_id":        cp.ID.Hex(),
				"code":       cp.Code,
				"bonus_days": cp.BonusDays,
				"max_uses":   cp.MaxUses,
				"uses":       cp.Uses,
				"used_by":    usedByStrs,
				"created_at": cp.CreatedAt.Format(time.RFC3339),
			}
			if cp.ExpiresAt != nil {
				item["expires_at"] = cp.ExpiresAt.Format(time.RFC3339)
			} else {
				item["expires_at"] = nil
			}
			output = append(output, item)
		}
		c.JSON(200, output)
		return
	}

	var body struct {
		Code      string     `json:"code" binding:"required"`
		BonusDays int        `json:"bonus_days" binding:"required"`
		MaxUses   *int       `json:"max_uses"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Validation error"})
		return
	}

	newC := Coupon{
		Code:      strings.ToUpper(strings.TrimSpace(body.Code)),
		BonusDays: body.BonusDays,
		MaxUses:   body.MaxUses,
		ExpiresAt: body.ExpiresAt,
		Uses:      0,
		UsedBy:    []primitive.ObjectID{},
		CreatedAt: time.Now().UTC(),
	}

	_, err := db.Collection("coupons").InsertOne(context.Background(), newC)
	if mongo.IsDuplicateKeyError(err) {
		c.JSON(409, gin.H{"msg": "Coupon code already exists"})
		return
	}
	c.JSON(201, gin.H{"msg": "Coupon created"})
}

// ==================== ADMIN: USER SEARCH ====================

func adminSearchUsers(c *gin.Context) {
	query := strings.TrimSpace(c.Query("q"))
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{}
	if query != "" {
		// Search by username (regex) or by ObjectID
		if oid, err := primitive.ObjectIDFromHex(query); err == nil {
			filter["_id"] = oid
		} else {
			filter["username"] = bson.M{"$regex": query, "$options": "i"}
		}
	}

	// Optional filters
	if status := c.Query("status"); status != "" {
		switch status {
		case "active":
			filter["expiryDate"] = bson.M{"$gt": time.Now().UTC()}
			filter["banned"] = bson.M{"$ne": true}
		case "expired":
			filter["$or"] = bson.A{
				bson.M{"expiryDate": bson.M{"$lte": time.Now().UTC()}},
				bson.M{"expiryDate": bson.M{"$exists": false}},
			}
			filter["banned"] = bson.M{"$ne": true}
		case "banned":
			filter["banned"] = true
		}
	}

	total, _ := db.Collection("users").CountDocuments(ctx, filter)
	skip := int64((page - 1) * limit)

	opts := options.Find().
		SetSort(bson.M{"created_at": -1}).
		SetSkip(skip).
		SetLimit(int64(limit)).
		SetProjection(bson.M{"password": 0, "session_salt": 0})

	cursor, err := db.Collection("users").Find(ctx, filter, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var users []User
	if err := cursor.All(ctx, &users); err != nil {
		c.JSON(500, gin.H{"msg": "Read error"})
		return
	}

	now := time.Now().UTC()
	output := make([]gin.H, 0, len(users))
	for _, u := range users {
		item := gin.H{
			"id":              u.ID.Hex(),
			"username":        u.Username,
			"role":            u.Role,
			"created_at":      u.CreatedAt.Format(time.RFC3339),
			"total_purchases": u.TotalPurchases,
			"coins":           u.Coins,
			"banned":          u.Banned,
		}
		if u.ExpiryDate != nil {
			item["expiry_date"] = u.ExpiryDate.Format(time.RFC3339)
			item["is_active"] = u.ExpiryDate.After(now)
			if u.ExpiryDate.After(now) {
				item["days_left"] = int(math.Ceil(u.ExpiryDate.Sub(now).Hours() / 24))
			}
		}
		if u.Banned {
			item["banned_by"] = u.BannedBy
			item["ban_reason"] = u.BanReason
			if u.BannedAt != nil {
				item["banned_at"] = u.BannedAt.Format(time.RFC3339)
			}
		}
		output = append(output, item)
	}

	c.JSON(200, gin.H{
		"users":  output,
		"total":  total,
		"page":   page,
		"limit":  limit,
		"pages":  int(math.Ceil(float64(total) / float64(limit))),
	})
}

// ==================== ADMIN: BAN / UNBAN ====================

func adminBanUser(c *gin.Context) {
	userID := c.Param("user_id")
	uVal, _ := c.Get("user")
	admin := uVal.(User)

	var body struct {
		Reason string `json:"reason"`
	}
	c.ShouldBindJSON(&body)
	if body.Reason == "" {
		body.Reason = "No reason provided"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oid, err := primitive.ObjectIDFromHex(userID)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid user ID"})
		return
	}

	// Don't allow banning yourself
	if oid == admin.ID {
		c.JSON(400, gin.H{"msg": "Cannot ban yourself"})
		return
	}

	now := time.Now().UTC()
	res, err := db.Collection("users").UpdateOne(ctx,
		bson.M{"_id": oid, "banned": bson.M{"$ne": true}},
		bson.M{"$set": bson.M{
			"banned":     true,
			"banned_at":  now,
			"banned_by":  admin.Username,
			"ban_reason": body.Reason,
		}},
	)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	if res.MatchedCount == 0 {
		// Check if user exists
		count, _ := db.Collection("users").CountDocuments(ctx, bson.M{"_id": oid})
		if count == 0 {
			c.JSON(404, gin.H{"msg": "User not found"})
		} else {
			c.JSON(409, gin.H{"msg": "User already banned"})
		}
		return
	}

	// Invalidate session by changing salt
	db.Collection("users").UpdateOne(ctx, bson.M{"_id": oid}, bson.M{
		"$set": bson.M{"session_salt": "banned_" + strconv.FormatInt(now.UnixNano(), 10)},
	})

	c.JSON(200, gin.H{"msg": "User banned", "username": userID})
}

func adminUnbanUser(c *gin.Context) {
	userID := c.Param("user_id")
	uVal, _ := c.Get("user")
	admin := uVal.(User)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oid, err := primitive.ObjectIDFromHex(userID)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid user ID"})
		return
	}

	now := time.Now().UTC()
	newSalt := strconv.FormatInt(now.UnixNano(), 10)

	res, err := db.Collection("users").UpdateOne(ctx,
		bson.M{"_id": oid, "banned": true},
		bson.M{
			"$set": bson.M{
				"banned":       false,
				"session_salt": newSalt,
			},
			"$unset": bson.M{
				"banned_at":  "",
				"banned_by":  "",
				"ban_reason": "",
			},
		},
	)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	if res.MatchedCount == 0 {
		count, _ := db.Collection("users").CountDocuments(ctx, bson.M{"_id": oid})
		if count == 0 {
			c.JSON(404, gin.H{"msg": "User not found"})
		} else {
			c.JSON(409, gin.H{"msg": "User is not banned"})
		}
		return
	}

	log.Printf("Admin %s unbanned user %s", admin.Username, userID)
	c.JSON(200, gin.H{"msg": "User unbanned successfully"})
}

// ==================== ADMIN: PURCHASE REPORTS ====================

func adminPurchaseReport(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now().UTC()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	// Total sales (approved transactions)
	totalSales, _ := db.Collection("transactions").CountDocuments(ctx, bson.M{"status": "approved"})

	var totalRevenue int64
	revAggCur, revAggErr := db.Collection("transactions").Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"status": "approved"}}},
		{{Key: "$group", Value: bson.M{"_id": nil, "total": bson.M{"$sum": "$amount"}}}},
	})
	if revAggErr == nil && revAggCur.Next(ctx) {
		var revResult struct {
			Total int64 `bson:"total"`
		}
		revAggCur.Decode(&revResult)
		totalRevenue = revResult.Total
		revAggCur.Close(ctx)
	}

	// New purchases this month (first-time buyers)
	newThisMonth, _ := db.Collection("transactions").CountDocuments(ctx, bson.M{
		"status":       "approved",
		"processed_at": bson.M{"$gte": monthStart},
	})

	// Renewals this month (users who had previous purchases)
	// Approximation: approved txs this month by users with total_purchases > 1
	renewalPipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"status":       "approved",
			"processed_at": bson.M{"$gte": monthStart},
		}}},
		{{Key: "$lookup", Value: bson.M{
			"from":         "users",
			"localField":   "user_id",
			"foreignField": "_id",
			"as":           "user_info",
		}}},
		{{Key: "$unwind", Value: "$user_info"}},
		{{Key: "$match", Value: bson.M{"user_info.total_purchases": bson.M{"$gt": 1}}}},
		{{Key: "$count", Value: "count"}},
	}
	renewalResult := struct {
		Count int64 `bson:"count"`
	}{}
	cur, err := db.Collection("transactions").Aggregate(ctx, renewalPipeline)
	if err == nil && cur.Next(ctx) {
		cur.Decode(&renewalResult)
	}
	cur.Close(ctx)

	// Today's stats
	todaySales, _ := db.Collection("transactions").CountDocuments(ctx, bson.M{
		"status":       "approved",
		"processed_at": bson.M{"$gte": todayStart},
	})

	// Pending count
	pendingCount, _ := db.Collection("transactions").CountDocuments(ctx, bson.M{"status": "pending"})

	// Active subscribers
	activeSubs, _ := db.Collection("users").CountDocuments(ctx, bson.M{
		"expiryDate": bson.M{"$gt": now},
		"banned":     bson.M{"$ne": true},
	})

	// Total users
	totalUsers, _ := db.Collection("users").CountDocuments(ctx, bson.M{})

	// Payment orders stats
	paidOrders, _ := db.Collection("payment_orders").CountDocuments(ctx, bson.M{"status": "paid"})
	paidThisMonth, _ := db.Collection("payment_orders").CountDocuments(ctx, bson.M{
		"status":  "paid",
		"paid_at": bson.M{"$gte": monthStart},
	})

	// Revenue from payment_orders this month
	var monthlyRevenue int64
	revCur, err := db.Collection("payment_orders").Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"status": "paid", "paid_at": bson.M{"$gte": monthStart}}}},
		{{Key: "$group", Value: bson.M{"_id": nil, "total": bson.M{"$sum": "$amount"}}}},
	})
	if err == nil && revCur.Next(ctx) {
		var res struct {
			Total int64 `bson:"total"`
		}
		revCur.Decode(&res)
		monthlyRevenue = res.Total
	}
	revCur.Close(ctx)

	c.JSON(200, gin.H{
		"overview": gin.H{
			"total_users":          totalUsers,
			"active_subscribers":   activeSubs,
			"total_transactions":   totalSales,
			"total_revenue_rial":   totalRevenue,
			"pending_transactions": pendingCount,
			"paid_payment_orders":  paidOrders,
		},
		"this_month": gin.H{
			"new_purchases":    newThisMonth,
			"renewals":         renewalResult.Count,
			"payment_orders":   paidThisMonth,
			"revenue_rial":     monthlyRevenue,
		},
		"today": gin.H{
			"approved_transactions": todaySales,
		},
		"generated_at": now.Format(time.RFC3339),
	})
}

// ==================== ADMIN: PLAN MANAGEMENT ====================

func adminListPlans(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options.Find().SetSort(bson.M{"sort_order": 1})
	cursor, err := db.Collection("plans").Find(ctx, bson.M{}, opts)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var plans []Plan
	if err := cursor.All(ctx, &plans); err != nil {
		c.JSON(500, gin.H{"msg": "Read error"})
		return
	}

	// If no plans in DB, return legacy plans
	if len(plans) == 0 {
		legacy := make([]gin.H, 0)
		for name, p := range LegacyPlans {
			legacy = append(legacy, gin.H{
				"name":   name,
				"days":   p.Days,
				"amount": p.Amount,
				"active": true,
				"source": "legacy",
			})
		}
		c.JSON(200, gin.H{"plans": legacy, "note": "Using legacy hardcoded plans. Create plans in DB to manage dynamically."})
		return
	}

	output := make([]gin.H, 0, len(plans))
	now := time.Now().UTC()
	for _, p := range plans {
		item := gin.H{
			"id":           p.ID.Hex(),
			"name":         p.Name,
			"display_name": p.DisplayName,
			"days":         p.Days,
			"amount":       p.Amount,
			"active":       p.Active,
			"sort_order":   p.SortOrder,
			"created_at":   p.CreatedAt.Format(time.RFC3339),
			"updated_at":   p.UpdatedAt.Format(time.RFC3339),
		}
		// Effective price with discount
		effAmount := getEffectiveAmount(&p)
		if effAmount != p.Amount {
			item["effective_amount"] = effAmount
			item["discount_percent"] = p.DiscountPercent
			if p.DiscountUntil != nil {
				item["discount_until"] = p.DiscountUntil.Format(time.RFC3339)
				item["discount_active"] = p.DiscountUntil.After(now)
			}
		}
		if len(p.QuantityDiscounts) > 0 {
			item["quantity_discounts"] = p.QuantityDiscounts
		}
		output = append(output, item)
	}

	c.JSON(200, gin.H{"plans": output})
}

func adminCreatePlan(c *gin.Context) {
	var body struct {
		Name              string             `json:"name" binding:"required"`
		DisplayName       string             `json:"display_name"`
		Days              int                `json:"days" binding:"required"`
		Amount            int64              `json:"amount" binding:"required"`
		Active            *bool              `json:"active"`
		SortOrder         int                `json:"sort_order"`
		DiscountPercent   int                `json:"discount_percent"`
		DiscountUntil     *time.Time         `json:"discount_until"`
		QuantityDiscounts []QuantityDiscount `json:"quantity_discounts"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Validation error", "detail": err.Error()})
		return
	}

	if body.Days < 1 || body.Days > 3650 {
		c.JSON(400, gin.H{"msg": "Days must be 1-3650"})
		return
	}
	if body.Amount < 1000 {
		c.JSON(400, gin.H{"msg": "Amount must be at least 1000 Rial"})
		return
	}

	name := strings.ToLower(strings.TrimSpace(body.Name))
	displayName := body.DisplayName
	if displayName == "" {
		displayName = name
	}

	active := true
	if body.Active != nil {
		active = *body.Active
	}

	now := time.Now().UTC()
	plan := Plan{
		Name:              name,
		DisplayName:       displayName,
		Days:              body.Days,
		Amount:            body.Amount,
		Active:            active,
		SortOrder:         body.SortOrder,
		DiscountPercent:   body.DiscountPercent,
		DiscountUntil:     body.DiscountUntil,
		QuantityDiscounts: body.QuantityDiscounts,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check duplicate name
	count, _ := db.Collection("plans").CountDocuments(ctx, bson.M{"name": name})
	if count > 0 {
		c.JSON(409, gin.H{"msg": "Plan name already exists"})
		return
	}

	res, err := db.Collection("plans").InsertOne(ctx, plan)
	if err != nil {
		c.JSON(500, gin.H{"msg": "Failed to create plan"})
		return
	}

	plan.ID = res.InsertedID.(primitive.ObjectID)
	c.JSON(201, gin.H{"msg": "Plan created", "plan": plan})
}

func adminUpdatePlan(c *gin.Context) {
	planID := c.Param("plan_id")
	oid, err := primitive.ObjectIDFromHex(planID)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid plan ID"})
		return
	}

	var body struct {
		DisplayName       *string            `json:"display_name"`
		Days              *int               `json:"days"`
		Amount            *int64             `json:"amount"`
		Active            *bool              `json:"active"`
		SortOrder         *int               `json:"sort_order"`
		DiscountPercent   *int               `json:"discount_percent"`
		DiscountUntil     *time.Time         `json:"discount_until"`
		QuantityDiscounts []QuantityDiscount `json:"quantity_discounts"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Invalid body"})
		return
	}

	update := bson.M{"$set": bson.M{"updated_at": time.Now().UTC()}}
	setFields := update["$set"].(bson.M)

	if body.DisplayName != nil {
		setFields["display_name"] = *body.DisplayName
	}
	if body.Days != nil {
		if *body.Days < 1 || *body.Days > 3650 {
			c.JSON(400, gin.H{"msg": "Days must be 1-3650"})
			return
		}
		setFields["days"] = *body.Days
	}
	if body.Amount != nil {
		if *body.Amount < 1000 {
			c.JSON(400, gin.H{"msg": "Amount must be at least 1000 Rial"})
			return
		}
		setFields["amount"] = *body.Amount
	}
	if body.Active != nil {
		setFields["active"] = *body.Active
	}
	if body.SortOrder != nil {
		setFields["sort_order"] = *body.SortOrder
	}
	if body.DiscountPercent != nil {
		setFields["discount_percent"] = *body.DiscountPercent
	}
	if body.DiscountUntil != nil {
		setFields["discount_until"] = *body.DiscountUntil
	}
	if body.QuantityDiscounts != nil {
		setFields["quantity_discounts"] = body.QuantityDiscounts
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := db.Collection("plans").UpdateOne(ctx, bson.M{"_id": oid}, update)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	if res.MatchedCount == 0 {
		c.JSON(404, gin.H{"msg": "Plan not found"})
		return
	}

	c.JSON(200, gin.H{"msg": "Plan updated"})
}

func adminDeletePlan(c *gin.Context) {
	planID := c.Param("plan_id")
	oid, err := primitive.ObjectIDFromHex(planID)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid plan ID"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Soft delete: just deactivate
	res, err := db.Collection("plans").UpdateOne(ctx, bson.M{"_id": oid}, bson.M{
		"$set": bson.M{"active": false, "updated_at": time.Now().UTC()},
	})
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	if res.MatchedCount == 0 {
		c.JSON(404, gin.H{"msg": "Plan not found"})
		return
	}

	c.JSON(200, gin.H{"msg": "Plan deactivated"})
}

// ==================== COIN SYSTEM ====================

func adminListCoinPackages(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := db.Collection("coin_packages").Find(ctx, bson.M{}, options.Find().SetSort(bson.M{"amount": 1}))
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	defer cursor.Close(ctx)

	var packages []CoinPackage
	cursor.All(ctx, &packages)

	c.JSON(200, gin.H{"packages": packages})
}

func adminCreateCoinPackage(c *gin.Context) {
	var body struct {
		Name   string `json:"name" binding:"required"`
		Coins  int64  `json:"coins" binding:"required"`
		Amount int64  `json:"amount" binding:"required"`
		Active *bool  `json:"active"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Validation error"})
		return
	}

	if body.Coins < 1 {
		c.JSON(400, gin.H{"msg": "Coins must be positive"})
		return
	}
	if body.Amount < 1000 {
		c.JSON(400, gin.H{"msg": "Amount must be at least 1000 Rial"})
		return
	}

	active := true
	if body.Active != nil {
		active = *body.Active
	}

	pkg := CoinPackage{
		Name:      body.Name,
		Coins:     body.Coins,
		Amount:    body.Amount,
		Active:    active,
		CreatedAt: time.Now().UTC(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := db.Collection("coin_packages").InsertOne(ctx, pkg)
	if err != nil {
		c.JSON(500, gin.H{"msg": "Failed to create package"})
		return
	}
	pkg.ID = res.InsertedID.(primitive.ObjectID)
	c.JSON(201, gin.H{"msg": "Coin package created", "package": pkg})
}

// User buys coins
func buyCoins(c *gin.Context) {
	uVal, _ := c.Get("user")
	user := uVal.(User)

	var body struct {
		PackageID string `json:"package_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "package_id required"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	pkgOID, err := primitive.ObjectIDFromHex(body.PackageID)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid package ID"})
		return
	}

	var pkg CoinPackage
	if err := db.Collection("coin_packages").FindOne(ctx, bson.M{"_id": pkgOID, "active": true}).Decode(&pkg); err != nil {
		c.JSON(404, gin.H{"msg": "Package not found or inactive"})
		return
	}

	merchant := os.Getenv("ZIBAL_MERCHANT")
	callback := os.Getenv("ZIBAL_CALLBACK_URL")
	if merchant == "" || callback == "" {
		c.JSON(500, gin.H{"msg": "Payment gateway not configured"})
		return
	}

	// Create payment order for coins
	order := PaymentOrder{
		UserID:     user.ID,
		Plan:       "coins_" + pkg.Name,
		Amount:     pkg.Amount,
		Status:     "created",
		Type:       "coins",
		CoinAmount: pkg.Coins,
		CreatedAt:  time.Now().UTC(),
	}

	res, err := db.Collection("payment_orders").InsertOne(ctx, order)
	if err != nil {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}
	oid := res.InsertedID.(primitive.ObjectID)

	payload := map[string]interface{}{
		"merchant":    merchant,
		"amount":      pkg.Amount,
		"callbackUrl": callback,
		"description": fmt.Sprintf("TwoManga Coins: %d (%s)", pkg.Coins, pkg.Name),
	}

	resp, err := zibalPost("v1/request", payload)
	if err != nil {
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{
			"$set": bson.M{"status": "failed", "zibal_error": err.Error()},
		})
		c.JSON(502, gin.H{"msg": "Gateway error"})
		return
	}

	resultStr := interfaceToString(resp["result"])
	if resultStr != "100" {
		msg := requestResult(resultStr)
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{
			"$set": bson.M{"status": "failed"},
		})
		c.JSON(400, gin.H{"msg": "Payment gateway rejected", "detail": msg})
		return
	}

	trackID := interfaceToString(resp["trackId"])
	if trackID == "" {
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{
			"$set": bson.M{"status": "failed"},
		})
		c.JSON(500, gin.H{"msg": "Gateway returned no trackId"})
		return
	}

	db.Collection("payment_orders").UpdateOne(context.Background(),
		bson.M{"_id": oid},
		bson.M{"$set": bson.M{"track_id": trackID, "status": "pending"}})

	redirectURL := os.Getenv("ZIBAL_GATEWAY_BASE")
	if redirectURL == "" {
		redirectURL = "https://gateway.zibal.ir"
	}
	redirectURL = strings.TrimRight(redirectURL, "/")

	c.JSON(200, gin.H{
		"redirect_url": redirectURL + "/start/" + trackID,
		"track_id":     trackID,
		"order_id":     oid.Hex(),
		"coins":        pkg.Coins,
		"amount":       pkg.Amount,
	})
}

// Get user's coin balance and history
func getCoinBalance(c *gin.Context) {
	uVal, _ := c.Get("user")
	user := uVal.(User)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var freshUser User
	db.Collection("users").FindOne(ctx, bson.M{"_id": user.ID}).Decode(&freshUser)

	// Get coin transactions
	cursor, err := db.Collection("transactions").Find(ctx,
		bson.M{"user_id": user.ID, "type": "coin_purchase"},
		options.Find().SetSort(bson.M{"created_at": -1}).SetLimit(20),
	)
	if err != nil {
		c.JSON(200, gin.H{"coins": freshUser.Coins, "history": []gin.H{}})
		return
	}
	defer cursor.Close(ctx)

	var txs []Transaction
	cursor.All(ctx, &txs)

	history := make([]gin.H, 0, len(txs))
	for _, tx := range txs {
		history = append(history, gin.H{
			"id":         tx.ID.Hex(),
			"amount":     tx.Amount,
			"status":     tx.Status,
			"created_at": tx.CreatedAt.Format(time.RFC3339),
		})
	}

	c.JSON(200, gin.H{
		"coins":   freshUser.Coins,
		"history": history,
	})
}

// ==================== ZIBAL HELPER ====================

func zibalPost(path string, payload interface{}) (map[string]interface{}, error) {
	base := os.Getenv("ZIBAL_GATEWAY_BASE")
	if base == "" {
		base = "https://gateway.zibal.ir"
	}
	base = strings.TrimRight(base, "/")

	client := &http.Client{Timeout: 15 * time.Second}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", base+"/"+strings.TrimLeft(path, "/"), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	var m map[string]interface{}
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func requestResult(result string) string {
	switch result {
	case "100":
		return "با موفقیت تایید شد."
	case "102":
		return "merchant یافت نشد."
	case "103":
		return "merchant غیرفعال است."
	case "104":
		return "merchant نامعتبر است."
	case "201":
		return "قبلا تایید شده."
	case "105":
		return "amount باید بزرگتر از 1,000 ریال باشد."
	case "106":
		return "callbackUrl نامعتبر است."
	case "113":
		return "مبلغ تراکنش از سقف مجاز بیشتر است."
	}
	return "خطا در درخواست پرداخت"
}

func verifyResult(result string) string {
	switch result {
	case "100":
		return "با موفقیت تایید شد."
	case "102":
		return "merchant یافت نشد."
	case "103":
		return "merchant غیر فعال"
	case "104":
		return "merchant نامعتبر"
	case "201":
		return "قبلا تایید شده."
	case "202":
		return "سفارش پرداخت نشده یا ناموفق بوده است."
	case "203":
		return "trackId نامعتبر است."
	}
	return "خطا در تایید پرداخت"
}

// ==================== PAYMENT ENDPOINTS ====================

func createPayment(c *gin.Context) {
	uVal, _ := c.Get("user")
	user := uVal.(User)

	var body struct {
		Plan string `json:"plan" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"msg": "Invalid request"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Get plan from DB (with legacy fallback)
	plan, err := getPlanByName(ctx, body.Plan)
	if err != nil {
		c.JSON(400, gin.H{"msg": "Invalid plan", "detail": err.Error()})
		return
	}

	effectiveAmount := getEffectiveAmount(plan)

	merchant := os.Getenv("ZIBAL_MERCHANT")
	callback := os.Getenv("ZIBAL_CALLBACK_URL")
	if merchant == "" || callback == "" {
		log.Println("ZIBAL envs missing")
		c.JSON(500, gin.H{"msg": "Payment gateway not configured"})
		return
	}

	order := PaymentOrder{
		UserID:    user.ID,
		Plan:      body.Plan,
		Amount:    effectiveAmount,
		Status:    "created",
		Type:      "subscription",
		CreatedAt: time.Now().UTC(),
	}

	res, err := db.Collection("payment_orders").InsertOne(ctx, order)
	if err != nil {
		log.Printf("insert payment order error: %v", err)
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}

	oid, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}

	payload := map[string]interface{}{
		"merchant":    merchant,
		"amount":      effectiveAmount,
		"callbackUrl": callback,
		"description": "TwoManga Plan: " + plan.DisplayName,
	}

	resp, err := zibalPost("v1/request", payload)
	if err != nil {
		log.Printf("zibal request error: %v", err)
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{
			"$set": bson.M{"status": "failed", "zibal_error": err.Error()},
		})
		c.JSON(502, gin.H{"msg": "Gateway error"})
		return
	}

	resultStr := interfaceToString(resp["result"])
	if resultStr != "100" {
		msg := requestResult(resultStr)
		log.Printf("zibal returned error: %v", resp)
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{
			"$set": bson.M{"status": "failed"},
		})
		c.JSON(400, gin.H{"msg": "Payment gateway rejected", "detail": msg})
		return
	}

	trackID := interfaceToString(resp["trackId"])
	if trackID == "" {
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": oid}, bson.M{
			"$set": bson.M{"status": "failed"},
		})
		c.JSON(500, gin.H{"msg": "Gateway returned no trackId"})
		return
	}

	db.Collection("payment_orders").UpdateOne(context.Background(),
		bson.M{"_id": oid},
		bson.M{"$set": bson.M{"track_id": trackID, "status": "pending"}})

	redirectURL := os.Getenv("ZIBAL_GATEWAY_BASE")
	if redirectURL == "" {
		redirectURL = "https://gateway.zibal.ir"
	}
	redirectURL = strings.TrimRight(redirectURL, "/")

	c.JSON(200, gin.H{
		"redirect_url": redirectURL + "/start/" + trackID,
		"track_id":     trackID,
		"order_id":     oid.Hex(),
		"plan":         plan.DisplayName,
		"amount":       effectiveAmount,
		"days":         plan.Days,
	})
}

func paymentCallback(c *gin.Context) {
	trackID := c.Query("trackId")
	if trackID == "" {
		c.JSON(400, gin.H{"msg": "Missing trackId"})
		return
	}

	merchant := os.Getenv("ZIBAL_MERCHANT")
	if merchant == "" {
		c.JSON(500, gin.H{"msg": "Gateway not configured"})
		return
	}

	payload := map[string]interface{}{
		"merchant": merchant,
		"trackId":  trackID,
	}
	resp, err := zibalPost("v1/verify", payload)
	if err != nil {
		log.Printf("zibal verify error: %v", err)
		c.JSON(502, gin.H{"msg": "Gateway verify error"})
		return
	}

	result := interfaceToString(resp["result"])
	if result != "100" {
		msg := verifyResult(result)
		log.Printf("zibal verify not success: %v - %v", result, resp)
		db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"track_id": trackID},
			bson.M{"$set": bson.M{"status": "failed", "zibal_verify": resp}})
		c.JSON(400, gin.H{"msg": "Payment not verified", "detail": msg})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var order PaymentOrder
	err = db.Collection("payment_orders").FindOne(ctx, bson.M{"track_id": trackID}).Decode(&order)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(404, gin.H{"msg": "Order not found - contact support"})
			return
		}
		c.JSON(500, gin.H{"msg": "DB error"})
		return
	}

	// Idempotency: if already paid, return success without re-processing
	if order.Status == "paid" {
		c.JSON(200, gin.H{"msg": "Already processed", "status": "paid"})
		return
	}

	amountReturned, ok := interfaceToInt64(resp["amount"])
	if ok && amountReturned != 0 && amountReturned != order.Amount {
		log.Printf("amount mismatch for order %s: returned=%d expected=%d", order.ID.Hex(), amountReturned, order.Amount)
		db.Collection("payment_orders").UpdateOne(ctx, bson.M{"_id": order.ID},
			bson.M{"$set": bson.M{"status": "failed", "zibal_verify": resp}})
		c.JSON(400, gin.H{"msg": "Amount mismatch - contact support"})
		return
	}

	var user User
	if err := db.Collection("users").FindOne(ctx, bson.M{"_id": order.UserID}).Decode(&user); err != nil {
		log.Printf("user not found when finalizing payment: %v", err)
		c.JSON(500, gin.H{"msg": "User not found"})
		return
	}

	now := time.Now().UTC()

	// Handle coin purchases
	if order.Type == "coins" && order.CoinAmount > 0 {
		_, err = db.Collection("users").UpdateOne(ctx, bson.M{"_id": user.ID}, bson.M{
			"$inc": bson.M{"coins": order.CoinAmount},
		})
		if err != nil {
			log.Printf("failed to add coins for user %s: %v", user.ID.Hex(), err)
			c.JSON(500, gin.H{"msg": "Failed to add coins"})
			return
		}

		// Record transaction
		db.Collection("transactions").InsertOne(ctx, Transaction{
			UserID:      user.ID,
			Username:    user.Username,
			TxHash:      trackID,
			Type:        "coin_purchase",
			Amount:      order.Amount,
			Status:      "approved",
			CreatedAt:   now,
			ProcessedAt: &now,
			ApprovedBy:  "system",
		})

		db.Collection("payment_orders").UpdateOne(ctx, bson.M{"_id": order.ID}, bson.M{
			"$set": bson.M{"status": "paid", "paid_at": now, "zibal_verify": resp},
		})

		c.JSON(200, gin.H{
			"msg":         "Coins added successfully",
			"coins_added": order.CoinAmount,
		})
		return
	}

	// Handle subscription
	start := now
	if user.ExpiryDate != nil && user.ExpiryDate.After(now) {
		start = *user.ExpiryDate
	}

	plan, err := getPlanByName(ctx, order.Plan)
	if err != nil {
		// Fallback
		plan = &Plan{Days: 30, Amount: order.Amount}
	}

	newExp := start.Add(time.Duration(plan.Days) * 24 * time.Hour)

	_, err = db.Collection("users").UpdateOne(ctx, bson.M{"_id": user.ID}, bson.M{
		"$set": bson.M{"expiryDate": newExp},
		"$inc": bson.M{"total_purchases": 1},
	})
	if err != nil {
		log.Printf("failed to update user expiry after payment: %v", err)
		c.JSON(500, gin.H{"msg": "Failed to update user"})
		return
	}

	// Record transaction
	db.Collection("transactions").InsertOne(ctx, Transaction{
		UserID:      user.ID,
		Username:    user.Username,
		TxHash:      trackID,
		Days:        plan.Days,
		Amount:      order.Amount,
		PlanName:    order.Plan,
		Type:        "subscription",
		Status:      "approved",
		CreatedAt:   now,
		ProcessedAt: &now,
		ApprovedBy:  "system",
	})

	_, err = db.Collection("payment_orders").UpdateOne(ctx, bson.M{"_id": order.ID}, bson.M{
		"$set": bson.M{"status": "paid", "paid_at": now, "zibal_verify": resp},
	})
	if err != nil {
		log.Printf("failed to update payment order as paid: %v", err)
	}

	c.JSON(200, gin.H{
		"msg":        "Payment successful",
		"new_expiry": newExp.Format(time.RFC3339),
		"plan":       plan.DisplayName,
		"days":       plan.Days,
	})
}

// ==================== RECONCILIATION ====================

func reconcilePendingPayments(interval time.Duration, minAge time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-shutdownCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			now := time.Now().UTC()
			threshold := now.Add(-minAge)
			cur, err := db.Collection("payment_orders").Find(ctx, bson.M{
				"status":     "pending",
				"created_at": bson.M{"$lt": threshold},
			})
			if err != nil {
				cancel()
				continue
			}

			var orders []PaymentOrder
			if err := cur.All(ctx, &orders); err != nil {
				cur.Close(ctx)
				cancel()
				continue
			}
			cur.Close(ctx)

			for _, order := range orders {
				payload := map[string]interface{}{
					"merchant": os.Getenv("ZIBAL_MERCHANT"),
					"trackId":  order.TrackID,
				}
				resp, err := zibalPost("v1/verify", payload)
				if err != nil {
					log.Printf("reconcile: verify error for order %s: %v", order.ID.Hex(), err)
					continue
				}
				result := interfaceToString(resp["result"])
				if result != "100" {
					db.Collection("payment_orders").UpdateOne(context.Background(), bson.M{"_id": order.ID},
						bson.M{"$set": bson.M{"status": "failed", "zibal_verify": resp}})
					continue
				}

				// BUG FIX: Use atomic update to prevent duplicate processing
				// Only process if still pending (atomic check-and-set)
				res, err := db.Collection("payment_orders").UpdateOne(context.Background(),
					bson.M{"_id": order.ID, "status": "pending"},
					bson.M{"$set": bson.M{"status": "paid", "paid_at": time.Now().UTC(), "zibal_verify": resp}},
				)
				if err != nil || res.MatchedCount == 0 {
					// Already processed by callback or another reconcile cycle
					continue
				}

				var user User
				if err := db.Collection("users").FindOne(context.Background(), bson.M{"_id": order.UserID}).Decode(&user); err != nil {
					log.Printf("reconcile: user not found for order %s: %v", order.ID.Hex(), err)
					continue
				}

				if order.Type == "coins" && order.CoinAmount > 0 {
					db.Collection("users").UpdateOne(context.Background(), bson.M{"_id": user.ID}, bson.M{
						"$inc": bson.M{"coins": order.CoinAmount},
					})
					db.Collection("transactions").InsertOne(context.Background(), Transaction{
						UserID:      user.ID,
						Username:    user.Username,
						TxHash:      order.TrackID,
						Type:        "coin_purchase",
						Amount:      order.Amount,
						Status:      "approved",
						CreatedAt:   time.Now().UTC(),
						ProcessedAt: func() *time.Time { t := time.Now().UTC(); return &t }(),
						ApprovedBy:  "reconcile",
					})
					log.Printf("reconcile: coins added for order %s", order.ID.Hex())
					continue
				}

				start := time.Now().UTC()
				if user.ExpiryDate != nil && user.ExpiryDate.After(start) {
					start = *user.ExpiryDate
				}
				plan, err := getPlanByName(context.Background(), order.Plan)
				if err != nil {
					plan = &Plan{Days: 30, Amount: order.Amount}
				}
				newExp := start.Add(time.Duration(plan.Days) * 24 * time.Hour)

				_, err = db.Collection("users").UpdateOne(context.Background(), bson.M{"_id": user.ID}, bson.M{
					"$set": bson.M{"expiryDate": newExp},
					"$inc": bson.M{"total_purchases": 1},
				})
				if err != nil {
					log.Printf("reconcile: failed to update user for order %s: %v", order.ID.Hex(), err)
					continue
				}

				db.Collection("transactions").InsertOne(context.Background(), Transaction{
					UserID:      user.ID,
					Username:    user.Username,
					TxHash:      order.TrackID,
					Days:        plan.Days,
					Amount:      order.Amount,
					PlanName:    order.Plan,
					Type:        "subscription",
					Status:      "approved",
					CreatedAt:   time.Now().UTC(),
					ProcessedAt: func() *time.Time { t := time.Now().UTC(); return &t }(),
					ApprovedBy:  "reconcile",
				})

				log.Printf("reconcile: order %s finalized (track=%s)", order.ID.Hex(), order.TrackID)
			}
			cancel()
		}
	}
}

// ==================== STARTUP ====================

func ensureIndexesAndAdmin() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if db == nil {
		log.Println("Skipping EnsureIndexes: DB not ready")
		return
	}

	// Users indexes
	db.Collection("users").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "username", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	db.Collection("users").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "banned", Value: 1}},
	})
	db.Collection("users").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "expiryDate", Value: 1}},
	})

	// Transactions indexes
	db.Collection("transactions").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "tx_hash", Value: 1}},
		Options: options.Index().SetUnique(true).SetSparse(true),
	})
	db.Collection("transactions").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}},
	})
	db.Collection("transactions").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: -1}},
	})
	db.Collection("transactions").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "type", Value: 1}},
	})

	// Coupons
	db.Collection("coupons").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "code", Value: 1}},
		Options: options.Index().SetUnique(true),
	})

	// Payment orders
	db.Collection("payment_orders").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "track_id", Value: 1}},
		Options: options.Index().SetUnique(true).SetSparse(true),
	})
	db.Collection("payment_orders").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}, {Key: "created_at", Value: -1}},
	})
	db.Collection("payment_orders").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "user_id", Value: 1}},
	})

	// Plans
	db.Collection("plans").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true),
	})

	// Coin packages
	db.Collection("coin_packages").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "active", Value: 1}},
	})

	// BUG FIX: properly handle FindOne errors
	if cfg.AdminEnvUser != "" && cfg.AdminEnvPass != "" {
		hash, _ := hashPassword(cfg.AdminEnvPass)
		userColl := db.Collection("users")
		err := userColl.FindOne(ctx, bson.M{"username": cfg.AdminEnvUser}).Err()
		if err == mongo.ErrNoDocuments {
			userColl.InsertOne(ctx, User{
				Username:    cfg.AdminEnvUser,
				Password:    hash,
				Role:        "admin",
				SessionSalt: "system",
				CreatedAt:   time.Now().UTC(),
				Coins:       0,
			})
			log.Println("Bootstrap admin created")
		} else if err != nil {
			log.Printf("WARNING: Error checking admin user: %v", err)
		}
	}

	// Seed default plans if collection is empty
	count, _ := db.Collection("plans").CountDocuments(ctx, bson.M{})
	if count == 0 {
		log.Println("Seeding default plans...")
		now := time.Now().UTC()
		defaultPlans := []Plan{
			{Name: "starter", DisplayName: "استارتر", Days: 30, Amount: 760000, Active: true, SortOrder: 1, CreatedAt: now, UpdatedAt: now},
			{Name: "pro", DisplayName: "حرفه‌ای", Days: 90, Amount: 2290000, Active: true, SortOrder: 2, CreatedAt: now, UpdatedAt: now},
			{Name: "elite", DisplayName: "الیت", Days: 180, Amount: 4290000, Active: true, SortOrder: 3, CreatedAt: now, UpdatedAt: now},
			{Name: "ultra", DisplayName: "اولترا", Days: 365, Amount: 8290000, Active: true, SortOrder: 4, CreatedAt: now, UpdatedAt: now},
		}
		for _, p := range defaultPlans {
			db.Collection("plans").InsertOne(ctx, p)
		}
		log.Println("Default plans seeded")
	}
}

func cleanupCouponsTask() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Cleanup Task Panic: %v", r)
		}
	}()

	ticker := time.NewTicker(1 * time.Hour)
	for {
		select {
		case <-shutdownCh:
			return
		case <-ticker.C:
			if db != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, _ = db.Collection("coupons").DeleteMany(ctx, bson.M{
					"expires_at": bson.M{"$lt": time.Now().UTC()},
				})
				cancel()
			}
		}
	}
}

// ==================== MAIN ====================

func main() {
	log.Println("Starting Server v2...")
	loadConfig()
	connectDB()

	heap.Init(&jobQueue)
	startWorkers(cfg.WorkerCount)

	// Start rate limiter cleanup
	rlAuthMe.Cleanup(10 * time.Minute)
	rlAuth.Cleanup(10 * time.Minute)
	rlGeneral.Cleanup(10 * time.Minute)
	rlAdmin.Cleanup(10 * time.Minute)
	rlPayment.Cleanup(10 * time.Minute)

	// Start maintenance tasks
	go func() {
		time.Sleep(1 * time.Second)
		ensureIndexesAndAdmin()
		go reconcilePendingPayments(5*time.Minute, 1*time.Minute)
		go cleanupCouponsTask()
	}()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	if origins := os.Getenv("FRONTEND_ORIGINS"); origins != "" {
		corsConfig.AllowAllOrigins = false
		corsConfig.AllowOrigins = strings.Split(origins, ",")
	}
	corsConfig.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	corsConfig.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	r.Use(cors.New(corsConfig))

	// Health check
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"service": "TwoManga API",
			"version": "2.0",
			"status":  "healthy",
			"workers": cfg.WorkerCount,
		})
	})

	// ===== AUTH ROUTES =====
	auth := r.Group("/auth")
	{
		// Strict rate limit for auth endpoints (per IP)
		auth.POST("/register",
			RateLimitMiddleware(rlAuth, func(c *gin.Context) string { return "reg_" + getClientIP(c) }),
			register,
		)
		auth.POST("/login",
			RateLimitMiddleware(rlAuth, func(c *gin.Context) string { return "login_" + getClientIP(c) }),
			login,
		)
		// Generous rate limit for /me (per user token) - 150/min
		auth.GET("/me",
			RateLimitMiddleware(rlAuthMe, func(c *gin.Context) string {
				return "me_" + c.GetHeader("Authorization")
			}),
			AuthMiddleware(false),
			getMe,
		)
	}

	// ===== USER ROUTES =====
	userGroup := r.Group("/user")
	userGroup.Use(
		RateLimitMiddleware(rlGeneral, func(c *gin.Context) string {
			return "user_" + c.GetHeader("Authorization")
		}),
		AuthMiddleware(false),
	)
	{
		userGroup.GET("/transactions", getUserTransactions)
		userGroup.GET("/coins", getCoinBalance)
	}

	// ===== PAYMENT ROUTES =====
	payment := r.Group("/payment")
	payment.Use(AuthMiddleware(false))
	{
		payment.POST("/submit",
			RateLimitMiddleware(rlPayment, func(c *gin.Context) string {
				return "pay_" + c.GetHeader("Authorization")
			}),
			submitPayment,
		)
		payment.POST("/create",
			RateLimitMiddleware(rlPayment, func(c *gin.Context) string {
				return "pay_" + c.GetHeader("Authorization")
			}),
			createPayment,
		)
		payment.POST("/buy-coins",
			RateLimitMiddleware(rlPayment, func(c *gin.Context) string {
				return "coin_" + c.GetHeader("Authorization")
			}),
			buyCoins,
		)
	}
	// Callback has no auth (called by gateway) but has IP-based rate limit
	r.GET("/payment/callback",
		RateLimitMiddleware(rlGeneral, func(c *gin.Context) string { return "cb_" + getClientIP(c) }),
		paymentCallback,
	)

	// ===== ADMIN ROUTES =====
	admin := r.Group("/admin")
	admin.Use(
		RateLimitMiddleware(rlAdmin, func(c *gin.Context) string {
			return "admin_" + c.GetHeader("Authorization")
		}),
		AuthMiddleware(true),
	)
	{
		// Transactions
		admin.GET("/transactions", adminListTx)
		admin.POST("/transactions/:tx_id/approve", adminApproveTx)
		admin.POST("/transactions/:tx_id/reject", adminRejectTx)

		// Coupons
		admin.GET("/coupons", manageCoupons)
		admin.POST("/coupons", manageCoupons)

		// User management
		admin.GET("/users/search", adminSearchUsers)
		admin.POST("/users/:user_id/ban", adminBanUser)
		admin.POST("/users/:user_id/unban", adminUnbanUser)

		// Reports
		admin.GET("/reports/purchases", adminPurchaseReport)

		// Plan management
		admin.GET("/plans", adminListPlans)
		admin.POST("/plans", adminCreatePlan)
		admin.PUT("/plans/:plan_id", adminUpdatePlan)
		admin.DELETE("/plans/:plan_id", adminDeletePlan)

		// Coin packages
		admin.GET("/coin-packages", adminListCoinPackages)
		admin.POST("/coin-packages", adminCreateCoinPackage)
	}

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen error: %s\n", err)
		}
	}()
	log.Printf("Server v2 listening on port %s", cfg.Port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down...")

	close(shutdownCh)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Println("Server Shutdown Force:", err)
	}

	if mongoClient != nil {
		mongoClient.Disconnect(context.Background())
		log.Println("DB Disconnected")
	}
	log.Println("Bye.")
}