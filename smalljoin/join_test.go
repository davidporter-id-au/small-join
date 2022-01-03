package smalljoin

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
			res, err := searchJSONWithQuery(td.input, td.jsonQuery)
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
		continueOnErr bool
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
		"found a JSON value in the second column, with valid JMESpath query string": {
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
		"error case: comman separator with JSON - can't be split correctly": {
			input: `{"data": ["123", "123"]}, blah`,
			queryoptions: QueryOptions{
				JsonSubquery: "data[0]",
				Separator:    ",",
				JoinColumn:   0,
			},
			expectedErr: errors.New("fatal error: it's not possible to split columns containing JSON with commas"),
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := attemptSplitAndSelectCol(td.input, td.queryoptions, td.continueOnErr)
			assert.Equal(t, td.expectedValue, res, name)
			assert.Equal(t, td.expectedErr, err, name)
		})
	}
}
