# ============================
# Stage 1: Builder (ساخت فایل اجرایی)
# ============================
# استفاده از آینه Arvancloud برای ایمیج Golang
FROM docker.arvancloud.ir/golang:1.25-alpine AS builder

# تنظیم آینه برای Alpine Linux در این مرحله
RUN sed -i 's/dl-cdn.alpinelinux.org/mirror.arvancloud.ir/g' /etc/apk/repositories && \
    apk update

# تنظیم GOPROXY و GOSUMDB برای دانلود سریعتر و مطمئن‌تر ماژول‌ها
# استفاده از آینه Abrha برای GOPROXY
ENV GOPROXY=https://mirror.abrha.net/repository/go/,direct
ENV GOSUMDB=off

# نصب Git برای دسترسی به ریپازیتوری‌ها در صورت نیاز (مثلاً برای ماژول‌های private)
# اطمینان از وجود git در محیط بیلد
RUN apk add --no-cache git

# تنظیم دایرکتوری کاری
WORKDIR /app

# کپی کردن فایل go.mod برای دانلود وابستگی‌ها به صورت مجزا
# این کار به Docker اجازه می‌دهد تا این لایه را کش کند اگر go.mod تغییر نکرده باشد.
COPY go.mod ./
# دانلود وابستگی‌ها
RUN go mod download

# کپی کردن بقیه سورس کد
COPY . .

# بیلد کردن برنامه نهایی
# CGO_ENABLED=0 برای اطمینان از اینکه باینری به صورت pure Go ساخته می‌شود
# GOOS=linux برای سیستم عامل هدف
# GOFLAGS=-mod=mod برای استفاده از go.mod به جای go.sum (اگر go.sum نباشد هم کار کند)
# -ldflags="-s -w" برای کاهش حجم باینری
RUN CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=mod go build -ldflags="-s -w" -o app .

# ============================
# Stage 2: Runner (محیط اجرای نهایی)
# ============================
# استفاده از آینه Arvancloud برای ایمیج Alpine
FROM docker.arvancloud.ir/library/alpine:latest AS runner

# تنظیم آینه برای Alpine Linux در این مرحله هم
RUN sed -i 's/dl-cdn.alpinelinux.org/mirror.arvancloud.ir/g' /etc/apk/repositories && \
    apk update

# نصب پکیج‌های ضروری برای اجرای برنامه:
# ca-certificates: برای ارتباطات HTTPS امن
# tzdata: برای تنظیمات منطقه زمانی (در صورت نیاز برنامه)
RUN apk add --no-cache ca-certificates tzdata

# ایجاد یک کاربر و گروه غیر روت برای افزایش امنیت
# -S برای ایجاد سیستم یوزر (بدون home directory)
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# تنظیم دایرکتوری کاری برای کاربر نهایی
WORKDIR /app

# کپی کردن فایل اجرایی بیلد شده از مرحله Builder
COPY --from=builder /app/app .

# تغییر مالکیت فایل اجرایی به کاربر غیر روت
RUN chown appuser:appgroup /app/app

# تغییر به کاربر غیر روت قبل از اجرای CMD
USER appuser

# اکسپوز کردن پورتی که برنامه شما روی آن اجرا می‌شود (اگر از پورت 10000 استفاده می‌کنید)
EXPOSE 10000

# دستور اجرای برنامه
CMD ["./app"]