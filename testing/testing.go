package testing

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"

	"github.com/knollit/coelacanth"
)

var (
	afterCallbacks []func() error
	commonDB       *sql.DB
)

type logWriter struct {
	*testing.T
}

func (l *logWriter) Write(p []byte) (n int, err error) {
	l.Log(string(p))
	return len(p), nil
}

// TestDB is a testing database connection. Most calls are proxied to an internal transaction so they can be rolled back after each test.
type TestDB struct {
	coelacanth.DB
	testTx *sql.Tx
}

// Begin proxies calls to the active transaction
func (db TestDB) Begin() (*sql.Tx, error) {
	return db.testTx, nil
}

// Close is a no-op. The connection is only closed at the conclusion of the test suite.
func (db TestDB) Close() error {
	return nil
}

// Exec proxies calls to the active transaction
func (db TestDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.testTx.Exec(query, args...)
}

// Prepare proxies calls to the active transaction
func (db TestDB) Prepare(query string) (*sql.Stmt, error) {
	return db.testTx.Prepare(query)
}

// Query proxies calls to the active transaction
func (db TestDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.testTx.Query(query, args...)
}

// QueryRow proxies calls to the active transaction
func (db TestDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.testTx.QueryRow(query, args...)
}

// RunAfterCallbacks executes all registered callbacks
func RunAfterCallbacks() {
	for _, cb := range afterCallbacks {
		if err := cb(); err != nil {
			fmt.Println(err)
		}
	}
}

// RegisterAfterCallback registers a callback to be run after the last test
func RegisterAfterCallback(cb func() error) {
	afterCallbacks = append(afterCallbacks, cb)
}

// RunWithDB executes testFunc with a prepared test database connection
func RunWithDB(t *testing.T, testFunc func(*TestDB)) {
	if commonDB == nil {
		db, _ := sql.Open("postgres", "user=ubuntu host=localhost dbname=postgres sslmode=disable")
		if err := db.Ping(); err != nil {
			t.Fatal("Error opening DB: ", err)
		}
		db.Exec("DROP DATABASE IF EXISTS endpoints_test")
		db.Exec("CREATE DATABASE endpoints_test")
		db.Close()
		commonDB, _ = sql.Open("postgres", "user=ubuntu host=localhost dbname=endpoints_test sslmode=disable")
		RegisterAfterCallback(func() error {
			return commonDB.Close()
		})
	}

	testDB := &TestDB{
		DB: commonDB,
	}
	setupSQL, _ := ioutil.ReadFile("db/db.sql")
	if _, err := testDB.DB.Exec(string(setupSQL)); err != nil {
		t.Fatal("Error setting up DB: ", err)
	}
	tx, err := testDB.DB.Begin()
	if err != nil {
		t.Fatal("Error starting TX: ", err)
	}
	testDB.testTx = tx
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Fatal("Error rolling back TX: ", err)
		}
	}()
	testFunc(testDB)
	return
}

// RunWithServer executes testFunc with a prepared server
func RunWithServer(t *testing.T, handler func(net.Conn, *coelacanth.Server), testFunc func(*coelacanth.Server, string)) {
	RunWithDB(t, func(db *TestDB) {
		// Setup server
		addrChan := make(chan string)
		conf := &coelacanth.Config{
			DB: db,
			ListenerFunc: func(addr string) (net.Listener, error) {
				l, err := net.Listen("tcp", addr)
				if err == nil {
					addrChan <- addr
				}
				return l, err
			},
			Logger: log.New(&logWriter{t}, "", log.Lmicroseconds),
		}
		s := coelacanth.NewServer(conf)
		defer func() {
			if err := s.Close(); err != nil {
				t.Fatal("Error closing server: ", err)
			}
		}()

		// Run server on a separate goroutine
		errs := make(chan error)
		go func() {
			errs <- s.Run(":13900", handler) // TODO not hardcoded
		}()
		select {
		case err := <-errs:
			t.Fatal(err)
		case <-time.NewTimer(5 * time.Second).C:
			t.Fatal("Timed out waiting for server to start")
		case addr := <-addrChan:
			testFunc(s, addr)
		}
	})
	return
}
