package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestSum(t *testing.T) {
	config := Config{Table:"table", FileName:"aaa", HasHeader:true}
	preset := Config{Schema:"schema", FileName:"bbb", HasHeader:false}

	config.FillMissingFromPreset(preset)
	assert.Equal(t, "schema", config.Schema)
	assert.Equal(t, "table", config.Table)
	assert.Equal(t, "aaa", config.FileName)
	assert.Equal(t, true, config.HasHeader)
}
