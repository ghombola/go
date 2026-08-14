# ============================
# Stage 1: Builder (ساخت فایل اجرایی)
# ============================
# استفاده از نسخه مشخص Golang (نسخه 1.25 یا بالاتر)
FROM docker.arvancloud.ir/golang:1.25-alpine AS builder

# تنظیم آینه برای Alpine Linux و نصب Git
RUN sed -i 's/dl-cdn.alpinelinux.org/mirror.arvancloud.ir/g' /etc/apk/repositories && \
    apk update && \
    apk add --no-cache git

# تنظیم GOPROXY و GOSUMDB
ENV GOPROXY=https://mirror.abrha.net/repository/go/,direct
ENV GOSUMDB=off

WORKDIR /app

# 🎯 بهینه‌سازی کش: کپی همزمان go.mod و go.sum
COPY go.mod go.sum ./
RUN go mod download

# کپی کردن بقیه سورس کد
COPY . .

# 🎯 بیلد استاتیک و بهینه:
# GOARCH=amd64 برای سرورهای x86_64 (اگر سرور ARM است، به arm64 تغییر دهید)
# -a -installsuffix cgo برای بیلد کامل و استاتیک
# -extldflags '-static' برای لینک استاتیک کتابخانه‌های C (جلوگیری از خطاهای glibc)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s -extldflags '-static'" -o app .

# ============================
# Stage 2: Runner (محیط اجرای نهایی)
# ============================
# 🎯 استفاده از نسخه مشخص Alpine (مثل 3.20 یا 3.21) به جای latest برای پایداری در پروداکشن
FROM docker.arvancloud.ir/library/alpine:3.21 AS runner

# تنظیم آینه و نصب پکیج‌های ضروری
RUN sed -i 's/dl-cdn.alpinelinux.org/mirror.arvancloud.ir/g' /etc/apk/repositories && \
    apk update && \
    apk add --no-cache ca-certificates tzdata

# ایجاد کاربر و گروه غیر روت
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# 🎯 کاهش لایه‌ها: تنظیم مالکیت در همان مرحله COPY (حذف یک لایه RUN اضافی)
COPY --from=builder --chown=appuser:appgroup /app/app .
COPY --from=builder --chown=appuser:appgroup /app/index.html .

# 🎯 تنظیم متغیرهای محیطی پیش‌فرض برای فریم‌ورک Gin (غیرفعال کردن حالت دیباگ)
ENV GIN_MODE=release

# تغییر به کاربر غیر روت
USER appuser

# اکسپوز کردن پورت
EXPOSE 10000

# 🎯 اضافه کردن Healthcheck برای مانیتورینگ خودکار توسط Docker/Kubernetes
# استفاده از wget که به صورت پیش‌فرض در Alpine وجود دارد (نیازی به نصب curl و افزایش حجم نیست)
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:10000/health || exit 1

# دستور اجرای برنامه
CMD ["./app"]
