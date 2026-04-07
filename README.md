# Hive v2

Оркестратор для нескольких libSQL-мастеров. Принимает клиентские подключения по нативному протоколу libSQL (Hrana v1/v2/v3 HTTP + WebSocket, gRPC embedded replica) и прозрачно распределяет запросы между мастерами по принципу «один владелец таблицы».

## Архитектура

```
                        ┌─────────────────────────────────────────────┐
                        │              Orchestrator :8080              │
                        │                                              │
  libsql client  ──────▶│  Hrana v1/v2/v3 HTTP   /v1/execute          │
  (HTTP / WS)           │  Hrana v3 WebSocket    ws://…               │
  go-libsql             │  gRPC proxy.Proxy      /proxy.Proxy/…       │
  embedded replica ────▶│  WAL replication       /wal_log.…  ─────────┼──▶ master[0]
                        │                                              │
                        │  Router ──▶ TableMap                        │
                        │  Replicator (async fan-out)                  │
                        └──────┬──────────────┬──────────────┬────────┘
                               │              │              │
                               ▼              ▼              ▼
                          master[0]      master[1]      master[2]
                         libsql-server  libsql-server  libsql-server
```

Подробнее: [docs/architecture.md](docs/architecture.md)

## Ключевые особенности

- **Table-owner sharding** — каждая таблица закреплена за одним мастером, конфликты записи исключены by design
- **Прозрачное проксирование** — клиенты подключаются как к обычному libSQL-серверу
- **Гибкие политики чтения** — `write_master`, `round_robin`, `random`
- **Двойной путь репликации** — SQL replay (Hrana) и raw gRPC forwarding
- **Единый порт** — HTTP/1.1, WebSocket и HTTP/2 (gRPC) через h2c
- **Минимальные зависимости** — 4 прямых Go-зависимости, без тяжёлых фреймворков

Подробнее: [docs/features.md](docs/features.md)

## Поддерживаемые протоколы

| Протокол | Эндпоинт | Описание |
|---|---|---|
| Hrana v1 HTTP | `POST /v1/execute`, `POST /v1/batch` | Stateless, без стримов |
| Hrana v2 HTTP | `POST /v2/pipeline` | Baton-сессии |
| Hrana v3 HTTP | `POST /v3/pipeline`, `POST /v3/cursor` | Стриминг курсоров |
| Hrana v3 WebSocket | `ws://…` (subprotocol `hrana3`) | Полный стриминг, курсоры |
| gRPC proxy.Proxy | `/proxy.Proxy/Execute` | Embedded replica записи |
| WAL replication | `/wal_log.ReplicationLog/…` | Прокси к master[0] |
| Health check | `GET /health` | 200 OK |
| Version | `GET /version` | Строка версии |
| Dump | `GET /dump` | Агрегированный SQL-дамп |

Подробнее: [docs/protocols.md](docs/protocols.md)

## Быстрый старт

### Docker Compose (рекомендуется)

```bash
docker compose up --build
```

Поднимает три libSQL-мастера (порты 18080–18082) и оркестратор на порту 8080.

### Локально

```bash
cp config.example.yaml config.yaml
go build -o orchestrator ./cmd/orchestrator
./orchestrator config.yaml
```

## Конфигурация

```yaml
listen_addr: ":8080"

masters:
  - url: "http://localhost:8081"
    token: "optional-auth-token"
  - url: "http://localhost:8082"

table_assignments:
  users: 0
  orders: 1

read_policy: "write_master"    # write_master | round_robin | random
stream_ttl: "10s"
max_body_bytes: 4194304        # 4 MiB

replication:
  workers: 4
  retry_max: 3
  retry_backoff: "1s"
  queue_capacity: 1024
```

Подробнее: [docs/requirements.md](docs/requirements.md)

## Подключение клиентов

### go-libsql (embedded replica)

```go
connector, err := goLibsql.NewEmbeddedReplicaConnector(
    "local.db",
    "http://localhost:8080",
)
db := sql.OpenDB(connector)
db.Exec("INSERT INTO orders (id, amount) VALUES (1, 100)")
connector.Sync()
db.QueryRow("SELECT amount FROM orders WHERE id = 1")
```

### libsql-client (HTTP / WebSocket)

```go
client, _ := libsql.NewClient("http://localhost:8080", libsql.WithAuthToken("..."))
client, _ := libsql.NewClient("ws://localhost:8080", libsql.WithAuthToken("..."))
```

### Прямой HTTP (Hrana v1)

```bash
curl -X POST http://localhost:8080/v1/execute \
  -H "Content-Type: application/json" \
  -d '{"stmt": {"sql": "SELECT 1"}}'
```

## Структура проекта

```
hive_v2/
├── cmd/orchestrator/       # Точка входа, DI, HTTP-сервер
├── internal/
│   ├── config/             # YAML-конфиг
│   ├── dump/               # Агрегация дампов
│   ├── grpcproxy/          # gRPC proxy.Proxy перехват
│   ├── hrana/              # Hrana HTTP сервер + клиент
│   ├── replication/        # Асинхронная репликация
│   ├── router/             # Маршрутизация + TableMap
│   ├── sql/                # SQL-анализатор
│   ├── stream/             # Baton-менеджер
│   └── ws/                 # Hrana v3 WebSocket
├── docs/                   # Документация
│   ├── architecture.md     # Архитектура
│   ├── protocols.md        # Протоколы
│   ├── modules.md          # Модули
│   ├── features.md         # Особенности
│   └── requirements.md     # Требования
├── config.example.yaml
├── config.docker.yaml
├── docker-compose.yml
└── Dockerfile
```

Подробнее: [docs/modules.md](docs/modules.md)

## Тестирование

```bash
go test ./...
go run ./cmd/replica_test/   # smoke-тест (требует запущенного compose)
```

## Ограничения

- **Кросс-мастерные транзакции** — транзакция, затрагивающая таблицы разных мастеров, завершится ошибкой. 2PC не реализован.
- **Репликация eventual consistency** — между записью и появлением данных на остальных мастерах есть задержка.
- **WAL-прокси только к master[0]** — embedded replica синхронизируются через master[0].
- **Без аутентификации на уровне оркестратора** — токены передаются напрямую мастерам.

## Документация

| Документ | Описание |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Архитектура, потоки данных, конкурентность |
| [docs/protocols.md](docs/protocols.md) | Все поддерживаемые протоколы и форматы |
| [docs/modules.md](docs/modules.md) | Описание каждого модуля проекта |
| [docs/features.md](docs/features.md) | Ключевые особенности и решения |
| [docs/requirements.md](docs/requirements.md) | Зависимости, runtime, конфигурация |
