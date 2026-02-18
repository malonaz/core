package grpcinproc

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type BidiStreamClient[Req, Resp any] interface {
	Send(*Req) error
	Recv() (*Resp, error)
	CloseSend() error
	grpc.ClientStream
}

func NewBidiStreamAsClient[Req, Resp any, Srv grpc.ServerStream](
	handler func(Srv) error,
) func(context.Context) (BidiStreamClient[Req, Resp], error) {
	return func(ctx context.Context) (BidiStreamClient[Req, Resp], error) {
		b := &bidiStream[Req, Resp]{
			ctx:      ctx,
			reqCh:    make(chan *Req),
			respCh:   make(chan respOrErr[Resp]),
			doneCh:   make(chan struct{}),
			sendDone: make(chan struct{}),
		}

		go func() {
			srv := &bidiServerAdapter[Req, Resp]{bidi: b}
			err := handler(any(srv).(Srv))
			if err != nil && err != io.EOF {
				select {
				case b.respCh <- respOrErr[Resp]{err: err}:
				case <-b.doneCh:
				}
			}
			close(b.respCh)
		}()

		return b, nil
	}
}

type respOrErr[T any] struct {
	resp *T
	err  error
}

type bidiStream[Req, Resp any] struct {
	ctx       context.Context
	reqCh     chan *Req
	respCh    chan respOrErr[Resp]
	doneCh    chan struct{}
	sendDone  chan struct{}
	closeOnce sync.Once
}

func (b *bidiStream[Req, Resp]) Context() context.Context { return b.ctx }

func (b *bidiStream[Req, Resp]) Send(req *Req) error {
	select {
	case <-b.sendDone:
		return errors.New("send on closed stream")
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.reqCh <- req:
		return nil
	}
}

func (b *bidiStream[Req, Resp]) Recv() (*Resp, error) {
	select {
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	case r, ok := <-b.respCh:
		if !ok {
			return nil, io.EOF
		}
		if r.err != nil {
			return nil, r.err
		}
		return r.resp, nil
	}
}

func (b *bidiStream[Req, Resp]) CloseSend() error {
	b.closeOnce.Do(func() {
		close(b.sendDone)
		close(b.reqCh)
	})
	return nil
}

func (b *bidiStream[Req, Resp]) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (b *bidiStream[Req, Resp]) Trailer() metadata.MD         { return metadata.MD{} }
func (b *bidiStream[Req, Resp]) SendMsg(m any) error {
	if msg, ok := m.(*Req); ok {
		return b.Send(msg)
	}
	return errors.New("SendMsg: invalid message type")
}
func (b *bidiStream[Req, Resp]) RecvMsg(m any) error {
	msg, err := b.Recv()
	if err != nil {
		return err
	}
	if ptr, ok := m.(*Resp); ok {
		*ptr = *msg
		return nil
	}
	return errors.New("RecvMsg: invalid message type")
}

type bidiServerAdapter[Req, Resp any] struct {
	bidi *bidiStream[Req, Resp]
}

func (a *bidiServerAdapter[Req, Resp]) Context() context.Context { return a.bidi.ctx }

func (a *bidiServerAdapter[Req, Resp]) Send(resp *Resp) error {
	select {
	case <-a.bidi.ctx.Done():
		return a.bidi.ctx.Err()
	case <-a.bidi.doneCh:
		return errors.New("client closed")
	case a.bidi.respCh <- respOrErr[Resp]{resp: resp}:
		return nil
	}
}

func (a *bidiServerAdapter[Req, Resp]) Recv() (*Req, error) {
	select {
	case <-a.bidi.ctx.Done():
		return nil, a.bidi.ctx.Err()
	case req, ok := <-a.bidi.reqCh:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	}
}

func (a *bidiServerAdapter[Req, Resp]) SetHeader(md metadata.MD) error  { return nil }
func (a *bidiServerAdapter[Req, Resp]) SendHeader(md metadata.MD) error { return nil }
func (a *bidiServerAdapter[Req, Resp]) SetTrailer(md metadata.MD)       {}
func (a *bidiServerAdapter[Req, Resp]) SendMsg(m any) error {
	if msg, ok := m.(*Resp); ok {
		return a.Send(msg)
	}
	return errors.New("SendMsg: invalid message type")
}
func (a *bidiServerAdapter[Req, Resp]) RecvMsg(m any) error {
	msg, err := a.Recv()
	if err != nil {
		return err
	}
	if ptr, ok := m.(*Req); ok {
		*ptr = *msg
		return nil
	}
	return errors.New("RecvMsg: invalid message type")
}
