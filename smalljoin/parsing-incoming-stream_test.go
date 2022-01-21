package smalljoin

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingIncomingStream(t *testing.T) {
	testdata, err := os.Open("internal/testdata/testdata")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)

	index := map[string]bool{}

	// build an index to compare against
	data, err := ioutil.ReadFile("internal/testdata/testdata")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	for d := range lines {
		index[lines[d]] = false
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
				t.Errorf("Could not find %v", v)
			}
			index[block[v]] = true
		}
	}
	for k, v := range index {
		assert.True(t, v, "could not find %v", k)
	}
	assert.NoError(t, err)
}

func TestParsingIncomingStream2(t *testing.T) {
	testdata, err := os.Open("internal/testdata/testdata2")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)

	index := map[string]bool{}

	// build an index to compare against
	data, err := ioutil.ReadFile("internal/testdata/testdata2")
	if err != nil {
		t.Fatalf("couldn't open testdata: %v", err)
	}
	lines := strings.Split(string(data), "\n")
	for d := range lines {
		index[strings.TrimSpace(lines[d])] = false
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
