package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// ==================== MAIN LAUNCHER ====================

func main() {
	log.Println("╔═══════════════════════════════════════════════════════╗")
	log.Println("║       TwoManga + Chess Room - Unified Server         ║")
	log.Println("║                   Version 5.0                        ║")
	log.Println("╚═══════════════════════════════════════════════════════╝")

	// ====== 1. بارگذاری پیکربندی ======
	loadConfig()

	// ====== 2. اتصال به دیتابیس ======
	connectDB()

	// ====== 3. راه‌اندازی Job Queue و Workers ======
	initWorkerSystem()

	// ====== 4. راه‌اندازی Rate Limiter ها ======
	initRateLimiters()

	// ====== 5. ساخت Index ها و Admin ======
	go func() {
		time.Sleep(1 * time.Second)
		ensureIndexesAndAdmin()
		go reconcilePendingPayments(5*time.Minute, 1*time.Minute)
		go cleanupCouponsTask()
	}()

	// ====== 6. ساخت Gin Router ======
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	// ====== 7. پیکربندی CORS ======
	configureCORS(r)

	// ====== 8. Root Endpoint ======
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"service":    "TwoManga + Chess API",
			"version":    "5.0",
			"status":     "healthy",
			"workers":    cfg.WorkerCount,
			"modules":    []string{"subscription", "coins", "chess"},
			"endpoints": gin.H{
				"auth":     "/auth/*",
				"user":     "/user/*",
				"payment":  "/payment/*",
				"admin":    "/admin/*",
				"chess":    "/chess/*",
				"chess_ws": "/chess/ws",
			},
		})
	})

	// ====== 9. ثبت مسیرهای سایت اصلی (site.go) ======
	setupSiteRoutes(r)
	log.Println("✓ Site routes registered (Auth, User, Payment, Admin)")

	// ====== 10. ثبت مسیرهای شطرنج (chess.go) ======
	SetupChessRoutes(r)
	log.Println("✓ Chess routes registered (Rooms, Games, WebSocket)")

	// ====== 11. Health Check Endpoint ======
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":    "ok",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"db":        checkDBHealth(),
			"modules": gin.H{
				"site":  true,
				"chess": true,
			},
		})
	})

	// ====== 12. راه‌اندازی HTTP Server ======
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("🚀 Server v5.0 listening on port %s", cfg.Port)
		log.Printf("📡 WebSocket available at ws://localhost:%s/chess/ws", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen error: %s\n", err)
		}
	}()

	// ====== 13. Graceful Shutdown ======
	waitForShutdown(srv)
}

// ==================== INITIALIZATION HELPERS ====================

func initWorkerSystem() {
	heap.Init(&jobQueue)
	startWorkers(cfg.WorkerCount)
	log.Printf("✓ Worker system initialized (%d workers)", cfg.WorkerCount)
}

func initRateLimiters() {
	rlAuthMe.Cleanup(10 * time.Minute)
	rlAuth.Cleanup(10 * time.Minute)
	rlGeneral.Cleanup(10 * time.Minute)
	rlAdmin.Cleanup(10 * time.Minute)
	rlPayment.Cleanup(10 * time.Minute)
	log.Println("✓ Rate limiters initialized and cleanup scheduled")
}

func configureCORS(r *gin.Engine) {
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true

	if origins := os.Getenv("FRONTEND_ORIGINS"); origins != "" {
		corsConfig.AllowAllOrigins = false
		corsConfig.AllowOrigins = strings.Split(origins, ",")
	}

	corsConfig.AllowHeaders = []string{
		"Origin", "Content-Length", "Content-Type", "Authorization",
		"X-Requested-With", "Accept",
	}
	corsConfig.AllowMethods = []string{
		"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH",
	}
	corsConfig.AllowCredentials = true
	corsConfig.MaxAge = 12 * time.Hour

	r.Use(cors.New(corsConfig))
}

func setupSiteRoutes(r *gin.Engine) {
	// ===== PUBLIC ROUTES =====
	r.GET("/plans", publicListPlans)
	r.GET("/coin-packages", publicListCoinPackages)

	// ===== AUTH ROUTES =====
	auth := r.Group("/auth")
	{
		auth.POST("/register",
			RateLimitMiddleware(rlAuth, func(c *gin.Context) string { return "reg_" + getClientIP(c) }),
			register,
		)
		auth.POST("/login",
			RateLimitMiddleware(rlAuth, func(c *gin.Context) string { return "login_" + getClientIP(c) }),
			login,
		)
		auth.GET("/me",
			RateLimitMiddleware(rlAuthMe, func(c *gin.Context) string {
				return tokenKeyHash("me_", c)
			}),
			AuthMiddleware(false),
			getMe,
		)
	}

	// ===== USER ROUTES =====
	userGroup := r.Group("/user")
	userGroup.Use(
		RateLimitMiddleware(rlGeneral, func(c *gin.Context) string {
			return tokenKeyHash("user_", c)
		}),
		AuthMiddleware(false),
	)
	{
		userGroup.GET("/transactions", getUserTransactions)
		userGroup.GET("/coins", getCoinBalance)
		userGroup.POST("/spend-coins", spendCoins)
	}

	// ===== PAYMENT ROUTES =====
	payment := r.Group("/payment")
	payment.Use(AuthMiddleware(false))
	{
		payment.POST("/submit",
			RateLimitMiddleware(rlPayment, func(c *gin.Context) string {
				return tokenKeyHash("pay_", c)
			}),
			submitPayment,
		)
		payment.POST("/create",
			RateLimitMiddleware(rlPayment, func(c *gin.Context) string {
				return tokenKeyHash("pay_", c)
			}),
			createPayment,
		)
		payment.POST("/buy-coins",
			RateLimitMiddleware(rlPayment, func(c *gin.Context) string {
				return tokenKeyHash("coin_", c)
			}),
			buyCoins,
		)
	}
	r.GET("/payment/callback",
		RateLimitMiddleware(rlGeneral, func(c *gin.Context) string { return "cb_" + getClientIP(c) }),
		paymentCallback,
	)

	// ===== CALLBACK HTML PAGE =====
	r.GET("/payment/result", func(c *gin.Context) {
		c.File("./index.html")
	})

	// ===== ADMIN ROUTES =====
	admin := r.Group("/admin")
	admin.Use(
		RateLimitMiddleware(rlAdmin, func(c *gin.Context) string {
			return tokenKeyHash("admin_", c)
		}),
		AuthMiddleware(true),
	)
	{
		admin.GET("/transactions", adminListTx)
		admin.POST("/transactions/:tx_id/approve", adminApproveTx)
		admin.POST("/transactions/:tx_id/reject", adminRejectTx)
		admin.GET("/coupons", adminListCoupons)
		admin.POST("/coupons", adminCreateCoupon)
		admin.DELETE("/coupons/:coupon_id", adminDeleteCoupon)
		admin.GET("/users/search", adminSearchUsers)
		admin.POST("/users/:user_id/ban", adminBanUser)
		admin.POST("/users/:user_id/unban", adminUnbanUser)
		admin.POST("/users/:user_id/cancel-subscription", adminCancelSubscription)
		admin.GET("/reports/purchases", adminPurchaseReport)
		admin.GET("/plans", adminListPlans)
		admin.POST("/plans", adminCreatePlan)
		admin.PUT("/plans/:plan_id", adminUpdatePlan)
		admin.DELETE("/plans/:plan_id", adminDeletePlan)
		admin.GET("/coin-packages", adminListCoinPackages)
		admin.POST("/coin-packages", adminCreateCoinPackage)
		admin.DELETE("/coin-packages/:pkg_id", adminDeleteCoinPackage)
	}
}

func checkDBHealth() gin.H {
	if db == nil {
		return gin.H{"status": "disconnected"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := mongoClient.Ping(ctx, nil)
	if err != nil {
		return gin.H{"status": "error", "error": err.Error()}
	}

	// شمارش collection ها
	collections := []string{"users", "chess_ratings", "chess_rooms", "chess_games"}
	stats := gin.H{}
	for _, col := range collections {
		count, _ := db.Collection(col).CountDocuments(ctx, bson.M{})
		stats[col] = count
	}

	return gin.H{
		"status":      "connected",
		"collections": stats,
	}
}

func waitForShutdown(srv *http.Server) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("")
	log.Println("⏹️  Shutdown signal received...")
	log.Println("┌─ Graceful shutdown initiated")

	close(shutdownCh)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// بستن WebSocket connections
	chessHub.mu.Lock()
	for _, session := range chessHub.rooms {
		session.mu.Lock()
		if session.WhiteConn != nil {
			session.WhiteConn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "Server shutting down"))
			session.WhiteConn.Close()
		}
		if session.BlackConn != nil {
			session.BlackConn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "Server shutting down"))
			session.BlackConn.Close()
		}
		for _, spectator := range session.Spectators {
			spectator.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "Server shutting down"))
			spectator.Close()
		}
		session.mu.Unlock()
	}
	chessHub.rooms = make(map[primitive.ObjectID]*RoomSession)
	chessHub.mu.Unlock()
	log.Println("├─ WebSocket connections closed")

	// بستن HTTP server
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("├─ Server Shutdown Force: %v", err)
	} else {
		log.Println("├─ HTTP server stopped")
	}

	// بستن اتصال دیتابیس
	if mongoClient != nil {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			log.Printf("├─ DB disconnect error: %v", err)
		} else {
			log.Println("├─ Database disconnected")
		}
	}

	log.Println("└─ Bye! 👋")
}
