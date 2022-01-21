package smalljoin

import (
	"io"
	"strings"
)

// finds the last newline and separates it out since we can't
// use a half-written line
func splitInputBytes(prevRemainder string, data []byte) ([]string, string) {
	dataString := string(data)
	cleanBlockIdx := strings.LastIndex(dataString, "\n")
	if cleanBlockIdx < 0 {
		return nil, dataString
	}
	// a clean block is a block of text which
	// finishes with newline, it may or may not
	// start partway thorough an existing line
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
		n, err := inputStream.Read(d)
		if io.EOF == err {
			if remainder != "" {
				j.incoming <- []string{strings.TrimSpace(remainder)}
			}
			break
		}
		if err != nil {
			panic(err)
		}

		data, newRemainder := splitInputBytes(remainder, d[:n])
		remainder = newRemainder
		j.incoming <- data
	}
	close(j.incoming)
	j.moreContent = false
	j.readWG.Done()
	return nil
}
