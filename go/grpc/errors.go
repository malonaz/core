package grpc

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Error struct {
	status *status.Status
}

func Errorf(code codes.Code, message string, params ...any) *Error {
	status := status.New(code, fmt.Sprintf(message, params...))
	return &Error{status: status}
}

func (e *Error) Err() error { return e.status.Err() }

func (e *Error) WithLocalizedMessage(message string, params ...any) *Error {
	localizedMessage := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: fmt.Sprintf(message, params...),
	}
	status, err := e.status.WithDetails(localizedMessage)
	if err != nil {
		log.Panicf("constructing status with details: %v", err)
	}
	e.status = status
	return e
}
