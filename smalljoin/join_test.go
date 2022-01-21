package smalljoin

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJoin(t *testing.T) {

	var leftSample = `{"data": "key-1"}`
	var leftSample2 = `{"data": "key-2"}`

	var rightSample = `right-join-data`
	index := rightIndex{"key-1": indexEntry{data: rightSample}}

	tests := map[string]struct {
		input         string
		jsonQuery     string
		expectedValue *Result
		expectedErr   error
	}{
		"found value with string": {
			input:     leftSample,
			jsonQuery: "data",
			expectedValue: &Result{
				Left: &LeftResult{
					Index: "key-1",
					Row:   leftSample,
				},
				Right: &RightResult{
					IndexFileResult: &IndexFileResult{
						Index: "key-1",
						Row:   rightSample,
					},
				},
			},
		},
		"value not present": {
			input:     leftSample2,
			jsonQuery: "data",
			expectedValue: &Result{
				Left: &LeftResult{
					Index: "key-2",
					Row:   leftSample2,
				},
				Right: nil,
			},
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {
			j := joiner{
				hashIndex: index,
				options: Options{
					IndexFile: "some file",
					LeftQueryOptions: QueryOptions{
						JsonSubquery: "data",
						JoinColumn:   -1,
					},
				},
			}
			res, err := j.join(td.input)

			assert.Equal(t, td.expectedValue, res, name)
			assert.Equal(t, td.expectedErr, err, name)
		})
	}
}

func TestJMESQueryStringQuery(t *testing.T) {

	tests := map[string]struct {
		input         string
		jsonQuery     string
		expectedValue string
		expectedErr   error
	}{
		"found value with int": {
			input:         `{"foo": {"bar": {"baz": [0, 1, 2, 3, 4]}}}`,
			jsonQuery:     "foo.bar.baz[2]",
			expectedValue: "2",
		},
		"found value with string": {
			input:         `{"data": "value"}`,
			jsonQuery:     "data",
			expectedValue: "value",
		},
		"value missing": {
			input:         `{"foo": {"bar": {"baz": [0, 1, 2, 3, 4]}}}`,
			jsonQuery:     "value.not.there",
			expectedValue: "",
		},
		"value struct": {
			input:         `{"foo": {"bar": {"baz": [0, 1, 2, 3, 4]}}}`,
			jsonQuery:     "foo",
			expectedValue: "",
			expectedErr:   errors.New("JMESpath query did not return a primitive type, this can't be joined on. Got: map[bar:map[baz:[0 1 2 3 4]]], type map[string]interface {}"),
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := searchJSONWithQuery(td.input, QueryOptions{JsonSubquery: td.jsonQuery})
			assert.Equal(t, td.expectedValue, res, name)
			assert.Equal(t, td.expectedErr, err, name)
		})
	}
}

func TestColSplitting(t *testing.T) {

	tests := map[string]struct {
		queryoptions  QueryOptions
		input         string
		expectedValue string
		expectedErr   error
	}{
		"text simple first column": {
			input: `E3A75F6C-03B6-4C52-B5A3-E2DECD75DA19, asdf`,
			queryoptions: QueryOptions{
				JsonSubquery: "",
				Separator:    ",",
				JoinColumn:   0,
			},
			expectedValue: "E3A75F6C-03B6-4C52-B5A3-E2DECD75DA19",
		},
		"text simple first column - whitespace handling": {
			input: ` E3A75F6C-03B6-4C52-B5A3-E2DECD75DA19 , asdf`,
			queryoptions: QueryOptions{
				JsonSubquery: "",
				Separator:    ",",
				JoinColumn:   0,
			},
			expectedValue: "E3A75F6C-03B6-4C52-B5A3-E2DECD75DA19",
		},
		"text simple whole column": {
			input: `E3A75F6C-03B6-4C52-B5A3-E2DECD75DA19, asdf`,
			queryoptions: QueryOptions{
				JsonSubquery: "",
				Separator:    "",
				JoinColumn:   -1,
			},
			expectedValue: "E3A75F6C-03B6-4C52-B5A3-E2DECD75DA19, asdf",
		},
		"found a JSON value in the second column, with valid JMESpath query string, double quoted csv seperator": {
			input: `some other column,"{""data\": ""value""}",blah`,
			queryoptions: QueryOptions{
				JsonSubquery:   "data",
				Separator:      ",",
				JoinColumn:     1,
				AttemptToClean: true,
			},
			expectedValue: "value",
		},
		"found a JSON value in the second column, with valid JMESpath query string, slash quoted csv seperator": {
			input: `some other column,"{\"data\": \"value\"}",blah`,
			queryoptions: QueryOptions{
				JsonSubquery:   "data",
				Separator:      ",",
				JoinColumn:     1,
				AttemptToClean: true,
			},
			expectedValue: "value",
		},
		"CSV separator with too few columns": {
			input: `1,2`,
			queryoptions: QueryOptions{
				JsonSubquery: "data",
				Separator:    ",",
				JoinColumn:   3,
			},
			expectedErr: errors.New("failure to parse CSV and fetch column 3, only found 2 columns. Data: 1,2"),
		},
		"all cols": {
			input: `{"data": "value"}`,
			queryoptions: QueryOptions{
				JsonSubquery: "data",
				Separator:    "",
				JoinColumn:   -1,
			},
			expectedValue: "value",
		},
		"found a JSON value in the second column, with valid JMESpath query string, pipe seperator": {
			input: `some other column | {"data": "value"} | blah`,
			queryoptions: QueryOptions{
				JsonSubquery: "data",
				Separator:    "|",
				JoinColumn:   1,
			},
			expectedValue: "value",
		},
		"found a JSON value in the first column, with valid JMESpath query string": {
			input: `{"data": "value"} | blah`,
			queryoptions: QueryOptions{
				JsonSubquery: "data",
				Separator:    "|",
				JoinColumn:   0,
			},
			expectedValue: "value",
		},
		"error case: index failure": {
			input: `{"data": ["123", "123"]} | blah`,
			queryoptions: QueryOptions{
				JsonSubquery: "data[0]",
				Separator:    "|",
				JoinColumn:   2,
			},
			expectedErr: errors.New("couldn't split row with separator | and get '2'th column. Only 2 columns found. Remember this is zero-based index. \n\nRow contents: {\"data\": [\"123\", \"123\"]} | blah"),
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := attemptSplitAndSelectCol(td.input, td.queryoptions)
			assert.Equal(t, td.expectedValue, res, name)
			assert.Equal(t, td.expectedErr, err, name)
		})
	}
}
