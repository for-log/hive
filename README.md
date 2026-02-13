# Hive

Распределённая SQL-система поверх SQLite

---

## Для чего нужен

SQLite отлично работает на одном узле, но не масштабируется горизонтально: нельзя писать в одну базу из нескольких процессов одновременно. Hive решает эту проблему:

- **Шардирование по таблицам**: каждая таблица принадлежит одному мастеру.
- **Чтение без сети**: каждое приложение держит локальную копию всех данных.
- **Стандартный интерфейс**: приложение использует обычный `database/sql` — никаких специальных API.

---

## Компоненты

```
┌─────────────────────────────────────────────────────────┐
│                    Go-приложение                        │
│              database/sql + hivedriver                  │
│         SELECT → локальный SQLite (без сети)            │
│         INSERT/UPDATE/DELETE/DDL → hive-router (gRPC)   │
└────────────────────┬──────────────────┬─────────────────┘
                     │ gRPC             │ HTTP (Sync)
              ┌──────▼──────┐           │
              │ hive-router │◄──────────┘
              │  (маршрут)  │
              └──┬──────┬───┘
          gRPC   │      │ gRPC
        ┌────────▼─┐  ┌─▼────────┐
        │ master-1 │  │ master-2 │
        │  SQLite  │  │  SQLite  │
        │ users,   │  │ orders,  │
        │ profiles │  │ payments │
        └──────────┘  └──────────┘
```

**hive-router** — центральный узел:
- Принимает SQL от приложений по gRPC.
- Знает, какая таблица на каком мастере (`CREATE TABLE` автоматически регистрирует таблицу).
- Маршрутизирует запросы к нужному мастеру.
- Отдаёт объединённый снапшот всех мастеров по HTTP.

**hive-master** — узел хранения:
- Хранит SQLite-базу с назначенными ему таблицами.
- Принимает запросы от роутера по gRPC.
- Поддерживает транзакции в рамках своих таблиц.
- Отдаёт снапшот своей базы по HTTP.

**hivedriver** — клиентский драйвер (`database/sql/driver`):
- `SELECT` → выполняется в локальном SQLite без сети.
- `INSERT`/`UPDATE`/`DELETE`/`DDL` → отправляется роутеру по gRPC.
- **Write-through**: после записи драйвер сразу применяет изменения к локальному SQLite (через `RETURNING`), не дожидаясь синхронизации.
- **Sync**: по таймеру или вручную скачивает объединённый снапшот с роутера и заменяет локальную копию.

---

## Принцип работы

### Запись

```
Приложение:  db.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "Alice")
                │
hivedriver:  → gRPC Execute к роутеру
                │
hive-router: → парсит SQL, находит таблицу "users", смотрит в meta-БД → master-1
             → пересылает запрос на master-1
             → получает вставленную строку (RETURNING *)
             → возвращает строку приложению
                │
hivedriver:  → применяет строку к локальному SQLite (write-through)
             → приложение сразу видит "Alice" в SELECT, без ожидания Sync
```

### Чтение

```
Приложение:  db.QueryContext(ctx, "SELECT * FROM users")
                │
hivedriver:  → выполняет запрос в локальном SQLite
             → возвращает результат (сеть не задействована)
```

### Синхронизация

```
connector.Sync(ctx)
    │
hivedriver:  → GET http://router:8080/db/merged
                │
hive-router: → параллельно скачивает снапшоты со всех мастеров
             → объединяет в один SQLite-файл
             → отдаёт клиенту
                │
hivedriver:  → заменяет содержимое локального SQLite данными из снапшота
```

### Транзакции

Транзакции поддерживаются в рамках одного мастера. Транзакции на несколько мастеров не поддерживается.

```go
tx, _ := db.BeginTx(ctx, nil)
tx.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "Bob")   // master-1
tx.ExecContext(ctx, "INSERT INTO profiles (user_id) VALUES (?)", 1) // master-1 — OK
tx.Commit()

tx2, _ := db.BeginTx(ctx, nil)
tx2.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "Carol")  // master-1
tx2.ExecContext(ctx, "INSERT INTO orders (user_id) VALUES (?)", 1)    // master-2 — ошибка!
```

---

## Быстрый старт

### Локально (без Docker)

```bash
make proto-gen

make build

make run-router

make run-master-1
make run-master-2

go run ./examples/app/main.go
```

### Docker Compose

```bash
# Только бэкенд (роутер + мастера)
docker compose -f docker-compose.back.yaml up

# Полный стек с примером приложения
docker compose -f docker-compose.example.yaml up
```

---

## Использование в приложении

```go
import (
    "database/sql"
    "hive/pkg/hivedriver"
)

// DSN: адрес роутера + путь к локальной БД + интервал синхронизации
connector, err := hivedriver.NewConnector(
    "localhost:9000?local_db=./local.db&sync_interval=30s",
)
if err != nil {
    log.Fatal(err)
}
db := sql.OpenDB(connector)
defer db.Close()

if err := connector.Sync(ctx); err != nil {
    log.Fatal(err)
}

db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`)

db.ExecContext(ctx, `INSERT INTO users (name) VALUES (?) RETURNING id, name`, "Alice")

rows, _ := db.QueryContext(ctx, `SELECT id, name FROM users`)
```

---

## Конфигурация

### Роутер (`configs/router.yaml`)

```yaml
grpc_addr: ":9000"
http_addr: ":8080"
meta_db_path: "./data/meta.db"
heartbeat_timeout: "30s"
tx_timeout: "30s"
log_level: "info"
```

### Мастер (`configs/master-1.yaml`)

```yaml
id: "master-1"
grpc_addr: ":9001"
http_addr: ":8081"
db_path: "./data/master-1.db"
snapshot_dir: "/tmp/hive-snapshots"
router_addr: "localhost:9000"
heartbeat_interval: "5s"
tx_timeout: "30s"
log_level: "info"
```
