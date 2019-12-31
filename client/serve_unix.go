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

	// read next from stream
	tryCopy := func(fd int, stream *smux.Stream) {
		size := stream.PeekSize()
		if size == 0 {
			return
		}
		buf := defaultAllocator.Get(size)
		nr, er := stream.TryRead(buf)
		if er != nil { // read error, delete
			delete(binds, stream)
			return
		}
		watcher.Write(fd, buf[:nr], chTx, stream)
	}

	for {
		select {
		case res := <-chTx:
			defaultAllocator.Put(res.Buffer)
			stream := res.Context.(*smux.Stream)

			if res.Err != nil { // write failed
				stream.Close()
				watcher.StopWatch(res.Fd)
				delete(binds, stream)
				continue
			}
			tryCopy(res.Fd, stream)
		case pair := <-chPair:
			fd, err := watcher.Watch(pair.conn)
			if err != nil {
				panic(err)
			}
			binds[pair.stream] = fd
			tryCopy(fd, pair.stream)
		case stream := <-chReadable:
			if fd, ok := binds[stream]; ok {
				tryCopy(fd, stream)
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
			sessions.Delete(s)
			return
		}

		for i := 0; i < n; i++ {
			chReadable <- events[i]
		}
	}
}

// handleClient aggregates connection p1 on mux with 'writeLock'
func handleClient(session *smux.Session, p1 net.Conn, quiet bool) {
	logln := func(v ...interface{}) {
		if !quiet {
			log.Println(v...)
		}
	}

	// global async-io
	gaioInit.Do(func() {
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

	// remember session
	_, ok := sessions.LoadOrStore(session, true)
	if !ok {
		go loopPoll(session)
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

	// p2 -> p1, async method
	chPair <- pair{p1, p2}

	// p1 -> p2, blocking method
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
