// Package eval judges chats against canonical evals and records each verdict on the
// judged resource: a filterable label holding the verdict and an annotation of the same
// key holding the evaluation.
//
//	var prematureHangUp = eval.Must("premature-hang-up")
//
//	judge := eval.NewJudge(aiServiceClient, model)
//	chat, err := judge.EvaluateChat(ctx, chatName, prematureHangUp)
//
//	// Chats that failed it: labels."eval.malonaz.com/premature-hang-up" = "fail".
//	evaluation, ok, err := evalpb.Annotations.Eval(prematureHangUp.GetId()).Get(chat)
package eval

import (
	"errors"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	evalpb "github.com/malonaz/core/genproto/ai/eval/v1"
	"github.com/malonaz/core/go/pbutil"
)

// Must returns the eval declared under id by a linked proto file, panicking if none or
// several declare it. Call it in a package-level var, so a removed or renamed eval fails at
// init; the declaring proto package must be imported for its file to be linked.
func Must(id string) *evalpb.Eval {
	return pbutil.Must(lookup(id))
}

func lookup(id string) (*evalpb.Eval, error) {
	var declared *evalpb.Eval
	var err error
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		var evals []*evalpb.Eval
		evals, err = pbutil.GetExtension[[]*evalpb.Eval](file.Options(), evalpb.E_Eval)
		if errors.Is(err, pbutil.ErrExtensionNotFound) {
			err = nil
			return true
		}
		if err != nil {
			return false
		}
		for _, eval := range evals {
			if eval.GetId() != id {
				continue
			}
			if declared != nil {
				err = fmt.Errorf("eval %q is declared more than once", id)
				return false
			}
			if err = protovalidate.Validate(eval); err != nil {
				err = fmt.Errorf("eval %q: %w", id, err)
				return false
			}
			declared = eval
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if declared == nil {
		return nil, fmt.Errorf("eval %q is not declared by any linked proto file", id)
	}
	return declared, nil
}
