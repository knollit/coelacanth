package coelacanth

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/flatbuffers/go"
	"github.com/mikeraimondi/prefixedio"
)

type Config struct {
	DB           DB
	ListenerFunc func(string) (net.Listener, error)
	Logger       *log.Logger
}

func NewServer(c *Config) *Server {
	return &Server{
		DB:         c.DB,
		listenFunc: c.ListenerFunc,
		Logger:     c.Logger,
		builderPool: sync.Pool{
			New: func() interface{} {
				return flatbuffers.NewBuilder(0)
			},
		},
		prefixedBufPool: sync.Pool{
			New: func() interface{} {
				return &prefixedio.Buffer{}
			},
		},
	}
}

type Server struct {
	Logger          *log.Logger
	DB              DB
	listenFunc      func(string) (net.Listener, error)
	builderPool     sync.Pool
	prefixedBufPool sync.Pool
	handler         func(net.Conn)
}

func (s *Server) Run(addr string, handler func(net.Conn, *Server)) error {
	attempts := 0
	for {
		if err := s.DB.Ping(); err != nil {
			attempts++
			if attempts >= 10 {
				return err
			}
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	listener, err := s.listenFunc(addr)
	if err != nil {
		return err
	}
	s.listenFunc = func(s string) (net.Listener, error) {
		return listener, nil
	}

	s.Logger.Printf("Listening for requests on %s...\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go handler(conn, s)
	}
}

func (s *Server) Close() error {
	listener, _ := s.listenFunc("")
	if err := listener.Close(); err != nil {
		s.Logger.Println("Failed to close TCP listener cleanly: ", err)
	}
	if err := s.DB.Close(); err != nil {
		s.Logger.Println("Failed to close database connection cleanly: ", err)
	}

	return nil
}

func (s *Server) GetBuilder() *flatbuffers.Builder {
	return s.builderPool.Get().(*flatbuffers.Builder)
}

func (s *Server) PutBuilder(b *flatbuffers.Builder) {
	s.builderPool.Put(b)
}

func (s *Server) GetPrefixedBuf() *prefixedio.Buffer {
	return s.prefixedBufPool.Get().(*prefixedio.Buffer)
}

func (s *Server) PutPrefixedBuf(b *prefixedio.Buffer) {
	s.prefixedBufPool.Put(b)
}
