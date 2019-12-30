// +build darwin netbsd freebsd openbsd dragonfly linux

package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/xtaci/gaio"
	"github.com/xtaci/kcptun/generic"
	"github.com/xtaci/smux"
)

const (
	gaioBufferSize = 65536
)

type pair struct {
	conn   net.Conn
	stream *smux.Stream
}

var (
	gaioInit   sync.Once
	chPair     chan pair
	chReadable chan *smux.Stream
	chTx       chan gaio.OpResult
	watcher    *gaio.Watcher
	sessions   sync.Map
)

func loopGaio() {
	binds := make(map[*smux.Stream]int)
	for {
		select {
		case res := <-chTx:
			defaultAllocator.Put(res.Buffer)
			stream := res.Context.(*smux.Stream)

			if res.Err != nil { // write failed
				stream.Close()
				watcher.StopWatch(res.Fd)
				continue
			}

			// read next from stream
			size := stream.PeekSize()
			if size == 0 {
				continue
			}
			buf := defaultAllocator.Get(size)
			nr, er := stream.TryRead(buf)
			if er == nil {
				watcher.Write(res.Fd, buf[:nr], chTx, stream)
			}
		case pair := <-chPair:
			fd, err := watcher.Watch(pair.conn)
			if err != nil {
				panic(err)
			}
			binds[pair.stream] = fd
			size := pair.stream.PeekSize()
			if size == 0 {
				continue
			}
			buf := defaultAllocator.Get(size)
			nr, er := pair.stream.TryRead(buf)
			if er == nil {
				watcher.Write(fd, buf[:nr], chTx, pair.stream)
			}
		case stream := <-chReadable:
			if fd, ok := binds[stream]; ok {
				size := stream.PeekSize()
				if size == 0 {
					continue
				}
				buf := defaultAllocator.Get(size)
				nr, er := stream.TryRead(buf)
				if er == nil {
					watcher.Write(fd, buf[:nr], chTx, stream)
				}
			}
		}
	}
}

// copy from stream to conn
func loopPoll(s *smux.Session) {
	events := make([]*smux.Stream, 128)
	for {
		n, err := s.PollWait(events)
		if err != nil {
			return
		}

		for i := 0; i < n; i++ {
			chReadable <- events[i]
		}
	}
}

// handleClient aggregates connection p1 on mux with 'writeLock'
func handleClient(session *smux.Session, p1 net.Conn, quiet bool) {
	gaioInit.Do(func() {
		// control struct init
		w, err := gaio.CreateWatcher(gaioBufferSize)
		if err != nil {
			panic(err)
		}

		chTx = make(chan gaio.OpResult)
		chReadable = make(chan *smux.Stream)
		chPair = make(chan pair)
		watcher = w

		go loopGaio()
	})

	_, ok := sessions.Load(session)
	if !ok {
		sessions.Store(session, true)
		go loopPoll(session)
	}

	logln := func(v ...interface{}) {
		if !quiet {
			log.Println(v...)
		}
	}

	p2, err := session.OpenStream()
	if err != nil {
		p1.Close()
		logln(err)
		return
	}
	defer p2.Close()

	logln("stream opened", "in:", p1.RemoteAddr(), "out:", fmt.Sprint(p2.RemoteAddr(), "(", p2.ID(), ")"))
	defer logln("stream closed", "in:", p1.RemoteAddr(), "out:", fmt.Sprint(p2.RemoteAddr(), "(", p2.ID(), ")"))

	chPair <- pair{p1, p2}
	// start tunnel & wait for tunnel termination
	streamCopy := func(dst io.Writer, src io.ReadCloser) {
		if _, err := generic.Copy(dst, src); err != nil {
			// report protocol error
			if err == smux.ErrInvalidProtocol {
				log.Println("smux", err, "in:", p1.RemoteAddr(), "out:", fmt.Sprint(p2.RemoteAddr(), "(", p2.ID(), ")"))
			}
		}
		p1.Close()
		p2.Close()
	}
	streamCopy(p2, p1)
}
