package smalljoin

import (
	"bytes"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type noopWriteCloser struct {
	*bytes.Buffer
}

func createNoopWriteCloser(b *bytes.Buffer) noopWriteCloser { return noopWriteCloser{b} }
func (n noopWriteCloser) Close() error                      { return nil }

func TestSimpleJoin(t *testing.T) {

	tests := map[string]struct {
		fileToStream   string
		indexFile      string
		expectedoutput string
		Options        Options
	}{
		"A simple plain JSON selection and csv index with inner join": {
			fileToStream: "internal/testdata/testdata_3",
			Options: Options{
				Jointype:    JoinTypeInner,
				Concurrency: 10,
				IndexFile:   "internal/testdata/index_3",
				RightQueryOptions: QueryOptions{
					Separator: ",",
				},
				LeftQueryOptions: QueryOptions{
					Separator:      ",",
					JsonSubquery:   "data.index",
					AttemptToClean: true,
					JoinColumn:     4,
				},
			},
			expectedoutput: `
{"Left":{"Index":"a","Row":"1,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"a\"}}\""},"Right":{"IndexFileResult":{"Index":"a","Row":"a"}}}
{"Left":{"Index":"b","Row":"2,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"b\"}}\""},"Right":{"IndexFileResult":{"Index":"b","Row":"b"}}}
			`,
		},
		"A simple plain JSON selection and csv index with left join": {
			fileToStream: "internal/testdata/testdata_3",
			Options: Options{
				Jointype:    JoinTypeLeft,
				Concurrency: 10,
				IndexFile:   "internal/testdata/index_3",
				RightQueryOptions: QueryOptions{
					Separator: ",",
				},
				LeftQueryOptions: QueryOptions{
					Separator:      ",",
					JsonSubquery:   "data.index",
					AttemptToClean: true,
					JoinColumn:     4,
				},
			},
			expectedoutput: `
{"Left":{"Index":"a","Row":"1,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"a\"}}\""},"Right":{"IndexFileResult":{"Index":"a","Row":"a"}}}
{"Left":{"Index":"b","Row":"2,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"b\"}}\""},"Right":{"IndexFileResult":{"Index":"b","Row":"b"}}}
{"Left":{"Index":"c","Row":"3,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"c\"}}\""},"Right":null}
{"Left":{"Index":"d","Row":"4,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"d\"}}\""},"Right":null}
			`,
		},
		"A simple plain JSON selection and csv index with right-is-null join": {
			fileToStream: "internal/testdata/testdata_3",
			Options: Options{
				Jointype:    JoinTypeRightIsNull,
				Concurrency: 10,
				IndexFile:   "internal/testdata/index_3",
				RightQueryOptions: QueryOptions{
					Separator: ",",
				},
				LeftQueryOptions: QueryOptions{
					Separator:      ",",
					JsonSubquery:   "data.index",
					AttemptToClean: true,
					JoinColumn:     4,
				},
			},
			expectedoutput: `
{"Left":{"Index":"c","Row":"3,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"c\"}}\""},"Right":null}
{"Left":{"Index":"d","Row":"4,col1,col2,\"test\",\"{\\\"data\\\": {\\\"index\\\":\\\"d\"}}\""},"Right":null}
			`,
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {

			outStream := createNoopWriteCloser(bytes.NewBuffer(nil))
			errStream := createNoopWriteCloser(bytes.NewBuffer(nil))
			inputStream, err := os.Open(td.fileToStream)
			if err != nil {
				t.FailNow()
			}

			j := New(inputStream, outStream, errStream, td.Options)

			err = j.Run()
			assert.NoError(t, err, name)

			sortAndCompare(t, td.expectedoutput, outStream.Bytes())
		})
	}
}

func sortAndCompare(t *testing.T, expected string, out []byte) {
	cutset := ` 	
`
	// output ordering isn't guaranteed
	outSlice := sort.StringSlice(strings.Split(string(out), "\n"))
	outSlice.Sort()
	outSorted := strings.Join(outSlice, "\n")

	assert.Equal(t, strings.Trim(expected, cutset), strings.Trim(string(outSorted), cutset))
}
