package smalljoin

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"
)

type Joiner interface {
	Run() error
}

type streams struct {
	input  io.ReadCloser
	output io.WriteCloser
	err    io.WriteCloser
}

type joiner struct {
	streams     streams
	errors      chan error
	incoming    chan []string
	readWG      sync.WaitGroup
	writeWG     sync.WaitGroup
	options     Options
	critLock    sync.RWMutex
	moreContent bool
	hashIndex   map[string]string
}

func New(inputstream io.ReadCloser, outputstream io.WriteCloser, errStream io.WriteCloser, o Options) Joiner {
	incomingBuffer := make(chan []string, o.IncomingBufferSize)
	errChan := make(chan error)
	var wg sync.WaitGroup

	if o.Concurrency == 0 {
		o.Concurrency = defaultConcurrency
	}
	if o.IncomingBufferSize == 0 {
		o.IncomingBufferSize = defaultInputByteLen
	}

	return &joiner{
		streams: streams{
			input:  inputstream,
			output: outputstream,
			err:    errStream,
		},
		readWG:      wg,
		errors:      errChan,
		incoming:    incomingBuffer,
		options:     o,
		moreContent: true,
	}
}

// finds the last newline and separates it out since we can't
// use a half-written line
func splitInputBytes(prevRemainder string, data []byte) ([]string, string) {
	dataString := string(data)
	cleanBlockIdx := strings.LastIndex(dataString, "\n")
	if cleanBlockIdx < 0 {
		return nil, dataString
	}
	cleanBlock := dataString[0:cleanBlockIdx]
	nextRemainder := dataString[cleanBlockIdx:]
	out := strings.Split(prevRemainder+cleanBlock, "\n")

	// this will have a leading newline, so remove it
	nextRemainder = strings.Replace(nextRemainder, "\n", "", 1)

	// remove whitespace on lines while are finished
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out, nextRemainder
}

// streams the input
func (j *joiner) readInput(inputStream io.ReadCloser) error {
	var d = make([]byte, defaultInputByteLen)
	var remainder string
	defer inputStream.Close()
	for {
		_, err := inputStream.Read(d)
		if io.EOF == err {
			break
		}
		if err != nil {
			panic(err)
		}

		data, newRemainder := splitInputBytes(remainder, d)
		remainder = newRemainder
		j.incoming <- data
	}
	close(j.incoming)
	j.moreContent = false
	j.readWG.Done()
	return nil
}

// for now, this just reads the right join file entirely into memory and builds an index
// in memory. This isn't going to work for large index files, so a future
// iteration of this will probably build an index which contains file-offsets.
// but for now this is the MVP
func createIndexMap(right string, queryOptions QueryOptions) (map[string]string, error) {
	d, err := ioutil.ReadFile(right)
	if err != nil {
		return nil, err
	}
	split := strings.Split(string(d), "\n")
	out := map[string]string{}
	for _, line := range split {
		k, err := attemptSplitAndSelectCol(line, queryOptions, false)
		if err != nil {
			return nil, err
		}
		out[k] = line
	}
	return out, nil
}

func (j *joiner) Run() error {
	i, err := createIndexMap(j.options.IndexFile, j.options.RightQueryOptions)
	if err != nil {
		return err
	}
	j.hashIndex = i

	j.readWG.Add(1)
	go j.readInput(j.streams.input)

	for i := 0; i < j.options.Concurrency; i++ {
		j.writeWG.Add(1)
		go j.process(i)
	}

	j.readWG.Wait()
	j.writeWG.Wait()
	j.drain()
	return nil
}

// takes a block of data and joins it from the incoming datastream
func (j *joiner) process(i int) {
	for {
		j.critLock.RLock()
		if !j.moreContent {
			j.critLock.RUnlock()
			break
		}
		j.critLock.RUnlock()
		datablock := <-j.incoming
		if len(datablock) == 0 {
			// channel is likely closed, nil data
			// so just hang tight and loop again in sec
			// to check if the content's done
			time.Sleep(time.Microsecond)
			continue
		}
		for _, line := range datablock {
			joinResult, err := j.join(line)
			if err != nil {
				log.Fatalf("fatal error in processing: %v", err)
			}
			j.writeOutResult(*joinResult)
		}
	}
	j.writeWG.Done()
}

func (j *joiner) drain() {
	for i := 0; i < len(j.incoming); i++ {
		datablock := <-j.incoming
		for _, line := range datablock {
			j.join(line)
		}
	}
}

func (j *joiner) writeOutResult(res Result) error {
	switch j.options.Jointype {
	case JoinTypeLeft:
		if res.right == nil {
			fmt.Printf("%s%s\n", *res.left, j.options.LeftQueryOptions.Separator)
			return nil
		}
		fmt.Printf("%s%s%s", *res.left, j.options.LeftQueryOptions.Separator, *res.right)
		return nil
	case JoinTypeInner:
		if res.left != nil && res.right != nil {
			fmt.Printf("\n%s%s%s", *res.left, j.options.LeftQueryOptions.Separator, *res.right)
		}
		j.debugPrint("no join", "%s\n", *res.left)
		return nil
	}
	return nil
}

func (j joiner) debugPrint(debugMsg string, fmtStr string, args ...interface{}) {
	if j.options.OutputDebugMode {
		// todo either use a real logging framework
		// or use string builder properly
		j.streams.err.Write([]byte(fmt.Sprintf("\033[33m"+debugMsg+"\033[0m:"+fmtStr, args...)))
	}
}
