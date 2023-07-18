package websocket

import (
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Server holds a websocket server connection.
type Server struct {
	conn        *websocket.Conn
	closeMutex  sync.Mutex
	name        string
	unmarshalFN UnmarshalFN
	onCloseFN   func(*Server)
	readChan    chan any
	writeChan   chan any
	close       chan struct{}
}

// StartServer starts a websocket server.
func StartServer(name string, w http.ResponseWriter, r *http.Request, unmarshalFN UnmarshalFN, onCloseFN func(*Server)) (*Server, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	server := &Server{
		conn:        conn,
		name:        name,
		unmarshalFN: unmarshalFN,
		onCloseFN:   onCloseFN,
		readChan:    make(chan any, readBufferSize),
		writeChan:   make(chan any, writeBufferSize),
		close:       make(chan struct{}),
	}
	go server.read()
	go server.write()
	return server, nil
}

// Close this server gracefully.
func (s *Server) Close() {
	s.closeMutex.Lock()
	defer s.closeMutex.Unlock()
	select {
	case <-s.close:
		// Do nothing - we're already closed.
	default:
		s.conn.Close()
		close(s.close)
		s.onCloseFN(s)
		log.Infof("Websocket server #%s closed", s.name)
	}
}

// Read returns the read channel.
func (s *Server) Read() <-chan any { return s.readChan }

// Write returns the write channel.
func (s *Server) Write() chan<- any { return s.writeChan }

func (s *Server) write() {
	for {
		select {
		case <-s.close:
			return
		case payload := <-s.writeChan:
			err := s.conn.WriteJSON(payload)
			if err != nil {
				log.Errorf("writing to websocket %#v", payload)
				log.Errorf("writing to websocket %s: %v", s.name, err)
				s.Close()
				return
			}
		}
	}
}

func (s *Server) read() {
	for {
		select {
		case <-s.close:
			return
		default:
		}
		_, bytes, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Errorf("reading message from %s: %v", s.name, err)
			}
			s.Close()
			return
		}
		payload, err := s.unmarshalFN(bytes)
		if err != nil {
			log.Errorf("unmarshaling message from %s: %v", s.name, err)
			continue
		}
		select {
		case s.readChan <- payload:
		default:
			log.Warningf("read channel for websocket %s is full", s.name)
			s.readChan <- payload // Wait for buffer to free up.
		}

	}
}
