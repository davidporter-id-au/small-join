package smalljoin

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/davidporter-id-au/small-join/smalljoin/csv"
	"github.com/jmespath/go-jmespath"
)

type Result struct {
	left  *string
	right *string
}

// Join is the main function which takes a string line from the input
// and attempts to match it against the index according to whatever settings
// are configured.
func (j *joiner) join(leftjoinRow string) (*Result, error) {
	leftJoinCell, err := attemptSplitAndSelectCol(leftjoinRow, j.options.LeftQueryOptions, j.options.continueOnErr)
	if err != nil {
		return nil, err
	}
	right, ok := j.hashIndex[leftJoinCell]
	if ok {
		return &Result{
			left:  &leftjoinRow,
			right: &right,
		}, nil
	}
	return &Result{
		left:  &leftjoinRow,
		right: nil,
	}, nil
}

// joinData is expected to be a json string such as `{"foo": {"bar": {"baz": [0, 1, 2, 3, 4]}}}`
// query: "foo.bar.baz[2]"
// return value: "" or 2
//
// The data queried out ideally should be a string, or at least will be attempted to be cast to a
// a string for the stake of joining.
func searchJSONWithQuery(jsonData string, query string) (string, error) {
	var data interface{}
	err := json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		return "", err
	}
	result, err := jmespath.Search(query, data)
	if err != nil {
		return "", err
	}
	switch v := result.(type) {
	case nil:
		return "", nil
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("JMESpath query did not return a primitive type, this can't be joined on. Got: %v, type %T", v, v)
	}
}

func attemptSplitAndSelectCol(row string, options QueryOptions, continueOnError bool) (string, error) {
	if options.Separator == "," && options.JsonSubquery != "" {
		cols, err := csv.BreakCSVIntoColumns(row)
		if err != nil {
			return "", err
		}
		if len(cols) < options.JoinColumn {
			return "", fmt.Errorf("attempted to index into CSV column %v, but only %v columns found: %v", options.JoinColumn, len(cols), cols)
		}
		return searchJSONWithQuery(cols[options.JoinColumn], options.JsonSubquery)
	}
	if options.JoinColumn < 0 {
		return strings.TrimSpace(row), nil
	}
	split := strings.Split(row, options.Separator)
	if len(split)-1 < options.JoinColumn {
		if continueOnError {
			return row, nil
		}
		return "", fmt.Errorf("couldn't split row with separator %s and get '%v'th column. Only %d columns found. Remember this is zero-based index. \n\nRow contents: %s", options.Separator, options.JoinColumn, len(split), row)
	}
	// no json involved, simple text case
	if options.JsonSubquery == "" {
		return strings.TrimSpace(split[options.JoinColumn]), nil
	}
	return searchJSONWithQuery(split[options.JoinColumn], options.JsonSubquery)
}
