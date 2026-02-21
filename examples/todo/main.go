package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"hive/pkg/hivedriver"
)

type ItemStatus string

const (
	ItemStatusPending ItemStatus = "pending"
	ItemStatusDone    ItemStatus = "done"
)

const maxRequestBodyBytes = 1 << 20 // 1 MB

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	grpcAddr := envOr("HIVE_GRPC_ADDR", "localhost:9000")
	httpAddr := envOr("HIVE_HTTP_ADDR", "localhost:8080")
	listenAddr := envOr("LISTEN_ADDR", ":8090")
	localDB := envOr("LOCAL_DB", "/tmp/hive-todo.db")

	syncInterval := envOr("SYNC_INTERVAL", "5s")
	dsn := fmt.Sprintf("%s?local_db=%s&http_addr=%s&sync_interval=%s&log_level=info", grpcAddr, localDB, httpAddr, syncInterval)
	connector, err := hivedriver.NewConnector(dsn)
	if err != nil {
		log.Error("new connector", "err", err)
		os.Exit(1)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := migrate(ctx, db); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /", swaggerUI)
	mux.HandleFunc("GET /openapi.json", openAPISpec)

	mux.HandleFunc("POST /users", handleCreateUser(db))
	mux.HandleFunc("GET /users", handleListUsers(db))

	mux.HandleFunc("POST /lists", handleCreateList(db))
	mux.HandleFunc("GET /lists", handleListLists(db))
	mux.HandleFunc("DELETE /lists/{id}", handleDeleteList(db))

	mux.HandleFunc("POST /lists/{id}/share", handleShareList(db))
	mux.HandleFunc("DELETE /lists/{id}/share/{user_id}", handleUnshareList(db))

	mux.HandleFunc("POST /lists/{id}/items", handleCreateItem(db))
	mux.HandleFunc("GET /lists/{id}/items", handleListItems(db))
	mux.HandleFunc("PATCH /items/{id}", handleUpdateItem(db))
	mux.HandleFunc("DELETE /items/{id}", handleDeleteItem(db))

	log.Info("listening", "addr", listenAddr, "swagger", "http://localhost"+listenAddr+"/")
	if err := http.ListenAndServe(listenAddr, mux); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT    NOT NULL UNIQUE
		)`,
		`CREATE TABLE IF NOT EXISTS lists (
			id      INTEGER PRIMARY KEY AUTOINCREMENT,
			owner   INTEGER NOT NULL,
			title   TEXT    NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS list_shares (
			list_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			PRIMARY KEY (list_id, user_id)
		)`,
		`CREATE TABLE IF NOT EXISTS items (
			id      INTEGER PRIMARY KEY AUTOINCREMENT,
			list_id INTEGER NOT NULL,
			text    TEXT    NOT NULL,
			status  TEXT    NOT NULL DEFAULT 'pending'
		)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("exec %q: %w", s, err)
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	d := json.NewDecoder(io.LimitReader(r.Body, maxRequestBodyBytes))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

func pathInt(r *http.Request, name string) (int64, error) {
	s := r.PathValue(name)
	if s == "" {
		return 0, fmt.Errorf("missing path param %q", name)
	}
	return strconv.ParseInt(s, 10, 64)
}

func errResp(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type User struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func handleCreateUser(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name string `json:"name"`
		}
		if err := readJSON(r, &body); err != nil || body.Name == "" {
			errResp(w, http.StatusBadRequest, "name required")
			return
		}
		row := db.QueryRowContext(r.Context(),
			`INSERT INTO users (name) VALUES (?) RETURNING id, name`, body.Name)
		var u User
		if err := row.Scan(&u.ID, &u.Name); err != nil {
			errResp(w, http.StatusConflict, "user already exists or db error: "+err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, u)
	}
}

func handleListUsers(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.QueryContext(r.Context(), `SELECT id, name FROM users ORDER BY id`)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		users := []User{}
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.Name); err != nil {
				errResp(w, http.StatusInternalServerError, err.Error())
				return
			}
			users = append(users, u)
		}
		writeJSON(w, http.StatusOK, users)
	}
}

type List struct {
	ID    int64  `json:"id"`
	Owner int64  `json:"owner"`
	Title string `json:"title"`
}

func handleCreateList(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Owner int64  `json:"owner"`
			Title string `json:"title"`
		}
		if err := readJSON(r, &body); err != nil || body.Owner == 0 || body.Title == "" {
			errResp(w, http.StatusBadRequest, "owner and title required")
			return
		}
		row := db.QueryRowContext(r.Context(),
			`INSERT INTO lists (owner, title) VALUES (?, ?) RETURNING id, owner, title`,
			body.Owner, body.Title)
		var l List
		if err := row.Scan(&l.ID, &l.Owner, &l.Title); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, l)
	}
}

func handleListLists(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userIDStr := r.URL.Query().Get("user_id")
		if userIDStr == "" {
			errResp(w, http.StatusBadRequest, "user_id query param required")
			return
		}
		userID, err := strconv.ParseInt(userIDStr, 10, 64)
		if err != nil {
			errResp(w, http.StatusBadRequest, "invalid user_id")
			return
		}
		rows, err := db.QueryContext(r.Context(), `
			SELECT DISTINCT l.id, l.owner, l.title
			FROM lists l
			LEFT JOIN list_shares s ON s.list_id = l.id
			WHERE l.owner = ? OR s.user_id = ?
			ORDER BY l.id`, userID, userID)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		lists := []List{}
		for rows.Next() {
			var l List
			if err := rows.Scan(&l.ID, &l.Owner, &l.Title); err != nil {
				errResp(w, http.StatusInternalServerError, err.Error())
				return
			}
			lists = append(lists, l)
		}
		writeJSON(w, http.StatusOK, lists)
	}
}

func handleDeleteList(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		for _, q := range []string{
			`DELETE FROM items WHERE list_id = ?`,
			`DELETE FROM list_shares WHERE list_id = ?`,
			`DELETE FROM lists WHERE id = ?`,
		} {
			if _, err := db.ExecContext(r.Context(), q, id); err != nil {
				errResp(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleShareList(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		listID, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		var body struct {
			UserID int64 `json:"user_id"`
		}
		if err := readJSON(r, &body); err != nil || body.UserID == 0 {
			errResp(w, http.StatusBadRequest, "user_id required")
			return
		}
		_, err = db.ExecContext(r.Context(),
			`INSERT OR IGNORE INTO list_shares (list_id, user_id) VALUES (?, ?)`,
			listID, body.UserID)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "shared"})
	}
}

func handleUnshareList(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		listID, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		userID, err := pathInt(r, "user_id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if _, err := db.ExecContext(r.Context(),
			`DELETE FROM list_shares WHERE list_id = ? AND user_id = ?`, listID, userID); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

type Item struct {
	ID     int64      `json:"id"`
	ListID int64      `json:"list_id"`
	Text   string     `json:"text"`
	Status ItemStatus `json:"status"`
}

func handleCreateItem(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		listID, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		var body struct {
			Text string `json:"text"`
		}
		if err := readJSON(r, &body); err != nil || body.Text == "" {
			errResp(w, http.StatusBadRequest, "text required")
			return
		}
		row := db.QueryRowContext(r.Context(),
			`INSERT INTO items (list_id, text, status) VALUES (?, ?, ?) RETURNING id, list_id, text, status`,
			listID, body.Text, ItemStatusPending)
		var item Item
		if err := row.Scan(&item.ID, &item.ListID, &item.Text, &item.Status); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, item)
	}
}

func handleListItems(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		listID, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		rows, err := db.QueryContext(r.Context(),
			`SELECT id, list_id, text, status FROM items WHERE list_id = ? ORDER BY id`, listID)
		if err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		items := []Item{}
		for rows.Next() {
			var item Item
			if err := rows.Scan(&item.ID, &item.ListID, &item.Text, &item.Status); err != nil {
				errResp(w, http.StatusInternalServerError, err.Error())
				return
			}
			items = append(items, item)
		}
		writeJSON(w, http.StatusOK, items)
	}
}

func handleUpdateItem(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		var body struct {
			Status *ItemStatus `json:"status"`
			Text   *string     `json:"text"`
		}
		if err := readJSON(r, &body); err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if body.Status != nil {
			if *body.Status != ItemStatusPending && *body.Status != ItemStatusDone {
				errResp(w, http.StatusBadRequest, "status must be 'pending' or 'done'")
				return
			}
			if _, err := db.ExecContext(r.Context(),
				`UPDATE items SET status = ? WHERE id = ?`, *body.Status, id); err != nil {
				errResp(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		if body.Text != nil {
			if _, err := db.ExecContext(r.Context(),
				`UPDATE items SET text = ? WHERE id = ?`, *body.Text, id); err != nil {
				errResp(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		row := db.QueryRowContext(r.Context(),
			`SELECT id, list_id, text, status FROM items WHERE id = ?`, id)
		var item Item
		if err := row.Scan(&item.ID, &item.ListID, &item.Text, &item.Status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				errResp(w, http.StatusNotFound, "item not found")
				return
			}
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, item)
	}
}

func handleDeleteItem(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := pathInt(r, "id")
		if err != nil {
			errResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if _, err := db.ExecContext(r.Context(), `DELETE FROM items WHERE id = ?`, id); err != nil {
			errResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func swaggerUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html>
<html>
<head>
  <title>Hive Todo API</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
  SwaggerUIBundle({
    url: "/openapi.json",
    dom_id: '#swagger-ui',
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
    layout: "BaseLayout",
    deepLinking: true
  })
</script>
</body>
</html>`)
}

func openAPISpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, openAPIJSON)
}

const openAPIJSON = `{
  "openapi": "3.0.3",
  "info": {
    "title": "Hive Todo API",
    "version": "1.0.0",
    "description": "Simple todo-list application backed by hive distributed SQLite."
  },
  "paths": {
    "/users": {
      "get": {
        "summary": "List all users",
        "operationId": "listUsers",
        "tags": ["users"],
        "responses": {
          "200": {"description": "OK", "content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/User"}}}}}
        }
      },
      "post": {
        "summary": "Create a user",
        "operationId": "createUser",
        "tags": ["users"],
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateUserRequest"}}}},
        "responses": {
          "201": {"description": "Created", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}},
          "409": {"description": "Already exists"}
        }
      }
    },
    "/lists": {
      "get": {
        "summary": "Lists owned by or shared with a user",
        "operationId": "listLists",
        "tags": ["lists"],
        "parameters": [{"name": "user_id", "in": "query", "required": true, "schema": {"type": "integer"}}],
        "responses": {
          "200": {"description": "OK", "content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/List"}}}}}
        }
      },
      "post": {
        "summary": "Create a list",
        "operationId": "createList",
        "tags": ["lists"],
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateListRequest"}}}},
        "responses": {
          "201": {"description": "Created", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/List"}}}}
        }
      }
    },
    "/lists/{id}": {
      "delete": {
        "summary": "Delete a list and all its items",
        "operationId": "deleteList",
        "tags": ["lists"],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "responses": {
          "204": {"description": "Deleted"}
        }
      }
    },
    "/lists/{id}/share": {
      "post": {
        "summary": "Share a list with a user",
        "operationId": "shareList",
        "tags": ["sharing"],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ShareRequest"}}}},
        "responses": {
          "200": {"description": "Shared"}
        }
      }
    },
    "/lists/{id}/share/{user_id}": {
      "delete": {
        "summary": "Revoke list access from a user",
        "operationId": "unshareList",
        "tags": ["sharing"],
        "parameters": [
          {"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}},
          {"name": "user_id", "in": "path", "required": true, "schema": {"type": "integer"}}
        ],
        "responses": {
          "204": {"description": "Revoked"}
        }
      }
    },
    "/lists/{id}/items": {
      "get": {
        "summary": "Get items in a list",
        "operationId": "listItems",
        "tags": ["items"],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "responses": {
          "200": {"description": "OK", "content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/Item"}}}}}
        }
      },
      "post": {
        "summary": "Add an item to a list",
        "operationId": "createItem",
        "tags": ["items"],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CreateItemRequest"}}}},
        "responses": {
          "201": {"description": "Created", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Item"}}}}
        }
      }
    },
    "/items/{id}": {
      "patch": {
        "summary": "Update item text or status",
        "operationId": "updateItem",
        "tags": ["items"],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/UpdateItemRequest"}}}},
        "responses": {
          "200": {"description": "Updated", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Item"}}}},
          "404": {"description": "Not found"}
        }
      },
      "delete": {
        "summary": "Delete an item",
        "operationId": "deleteItem",
        "tags": ["items"],
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "responses": {
          "204": {"description": "Deleted"}
        }
      }
    }
  },
  "components": {
    "schemas": {
      "User": {
        "type": "object",
        "properties": {
          "id":   {"type": "integer"},
          "name": {"type": "string"}
        }
      },
      "CreateUserRequest": {
        "type": "object",
        "required": ["name"],
        "properties": {
          "name": {"type": "string", "example": "alice"}
        }
      },
      "List": {
        "type": "object",
        "properties": {
          "id":    {"type": "integer"},
          "owner": {"type": "integer"},
          "title": {"type": "string"}
        }
      },
      "CreateListRequest": {
        "type": "object",
        "required": ["owner", "title"],
        "properties": {
          "owner": {"type": "integer", "example": 1},
          "title": {"type": "string", "example": "Shopping"}
        }
      },
      "ShareRequest": {
        "type": "object",
        "required": ["user_id"],
        "properties": {
          "user_id": {"type": "integer", "example": 2}
        }
      },
      "Item": {
        "type": "object",
        "properties": {
          "id":      {"type": "integer"},
          "list_id": {"type": "integer"},
          "text":    {"type": "string"},
          "status":  {"type": "string", "enum": ["pending", "done"]}
        }
      },
      "CreateItemRequest": {
        "type": "object",
        "required": ["text"],
        "properties": {
          "text": {"type": "string", "example": "Buy milk"}
        }
      },
      "UpdateItemRequest": {
        "type": "object",
        "properties": {
          "text":   {"type": "string"},
          "status": {"type": "string", "enum": ["pending", "done"]}
        }
      }
    }
  }
}`

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
