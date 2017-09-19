package qfilter_metrics

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestRewriteDims(t *testing.T) {
	dims := map[string]string{
		"dim1": "val1",
		"dim2": "val2",
	}
	rw := map[string]string{
		"dim2": "dim3",
	}
	exp := map[string]string{
		"dim1": "val1",
		"dim3": "val2",
	}
	got := RewriteDims(rw, dims)
	assert.Equal(t, exp, got)

}
