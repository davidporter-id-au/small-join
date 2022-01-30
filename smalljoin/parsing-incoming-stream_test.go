package smalljoin

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The idea: parse in the file both as a stream and check off and ensure that every
// entry in the incoming stream is found in the file and is the same.
// this tests issues around the incoming parser having to deal with chunks of
// data and reconstruct them correctly
func TestParsingIncomingStream(t *testing.T) {
	testdata, err := os.Open("internal/testdata/testdata_1")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)

	index := map[string]bool{}

	// build an index to compare against
	data, err := ioutil.ReadFile("internal/testdata/testdata_1")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	for d := range lines {
		// we shall ignore empty newlines
		if lines[d] != "" {
			index[lines[d]] = false
		}
	}

	joiner := joiner{
		streams: streams{
			input: testdata,
		},
		incoming: make(chan []string, 6000),
		readWG:   wg,
	}
	err = joiner.readInput(testdata)
	for block := range joiner.incoming {
		for v := range block {
			_, ok := index[block[v]]
			if !ok {
				t.Errorf("Could not find value in index, which might indicate a malformed parse or a mismatch between index and test %q", v)
			}
			index[block[v]] = true
		}
	}
	for k, v := range index {
		assert.True(t, v, "could not find value parsed in from incoming stream %q, %q", k)
	}
	assert.NoError(t, err)
}

func TestParsingIncomingStream2(t *testing.T) {
	testdata, err := os.Open("internal/testdata/testdata_2")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)

	index := map[string]bool{}

	// build an index to compare against
	data, err := ioutil.ReadFile("internal/testdata/testdata_2")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	for d := range lines {
		if lines[d] != "" {
			index[strings.TrimSpace(lines[d])] = false
		}
	}

	joiner := joiner{
		streams: streams{
			input: testdata,
		},
		incoming: make(chan []string, 6000),
		readWG:   wg,
	}
	err = joiner.readInput(testdata)
	for block := range joiner.incoming {
		for v := range block {
			_, ok := index[block[v]]
			if !ok {
				t.Errorf("unexpected parsed value: %q", v)
			}
			index[block[v]] = true
		}
	}
	for k, v := range index {
		if !v {
			t.Errorf("expected, but did not find value from index; %q", k)
		}
	}
	assert.NoError(t, err)
}
