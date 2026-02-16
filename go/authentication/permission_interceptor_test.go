package authentication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileWildcardPermission(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		match   bool
	}{
		{"/foo.bar.Service/*", "/foo.bar.Service/GetThing", true},
		{"/foo.bar.Service/*", "/foo.bar.Service/ListThings", true},
		{"/foo.bar.Service/*", "/foo.bar.OtherService/GetThing", false},
		{"/foo.bar.*/GetThing", "/foo.bar.Service/GetThing", true},
		{"/foo.bar.*/GetThing", "/foo.bar.Other/GetThing", true},
		{"/foo.bar.*/GetThing", "/foo.bar.Service/ListThings", false},
		{"/*", "/foo.bar.Service/GetThing", true},
		{"/foo.bar.Service/GetThing", "/foo.bar.Service/GetThing", true},
		{"/foo.bar.Service/GetThing", "/foo.bar.Service/ListThings", false},
		{"/foo.*.Service/*", "/foo.bar.Service/GetThing", true},
		{"/foo.*.Service/*", "/foo.baz.Service/ListThings", true},
		{"/foo.*.Service/*", "/foo.bar.Other/GetThing", false},
	}

	for _, test := range tests {
		t.Run(test.pattern+"_"+test.input, func(t *testing.T) {
			compiled, err := compileWildcardPermission(test.pattern)
			require.NoError(t, err)
			require.Equal(t, test.match, compiled.MatchString(test.input))
		})
	}
}
