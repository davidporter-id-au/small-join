package smalljoin

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/jmespath/go-jmespath"
)

// matches \"
var slashEscapeForQuotesRE = regexp.MustCompile(`\\\"`)
var doubleQuotes = regexp.MustCompile(`""`)

// Join is the main function which takes a string line from the input
// and attempts to match it against the index according to whatever settings
// are configured.
func (j *joiner) join(leftjoinRow string) (*Result, error) {
	if j.options.IndexFile != "" {
		return j.joinIndexFile(leftjoinRow)
	}
	if j.options.RightExecStr != "" {
		return j.joinExecStr(leftjoinRow)
	}
	panic("no configured joining options")
}

func (j *joiner) joinIndexFile(leftjoinRow string) (*Result, error) {
	leftJoinCell, err := attemptSplitAndSelectCol(leftjoinRow, j.options.LeftQueryOptions)
	if err != nil {
		return nil, err
	}
	if leftJoinCell == "" {
		return &Result{}, nil
	}
	right, ok := j.hashIndex[leftJoinCell]
	if ok {
		atomic.AddInt32(&right.joinCount, 1)
		return &Result{
			Left: &LeftResult{
				Row:   leftjoinRow,
				Index: leftJoinCell,
			},
			Right: &RightResult{
				IndexFileResult: &IndexFileResult{
					Index: leftJoinCell,
					Row:   right.data,
				},
			},
		}, nil
	}
	return &Result{
		Left: &LeftResult{
			Index: leftJoinCell,
			Row:   leftjoinRow,
		},
		Right: nil,
	}, nil
}

func (j *joiner) joinExecStr(leftjoinRow string) (*Result, error) {
	leftJoinCell, err := attemptSplitAndSelectCol(leftjoinRow, j.options.LeftQueryOptions)
	if err != nil {
		return nil, err
	}
	if leftJoinCell == "" {
		return &Result{}, nil
	}

	cmd := exec.Command("bash", "-c", strings.ReplaceAll(j.options.RightExecStr, "{}", leftJoinCell))
	stdout, err := cmd.CombinedOutput()
	stdOutStr := string(stdout)
	if err != nil {
		var e *exec.ExitError
		if errors.As(err, &e) {
			code := e.ProcessState.ExitCode()
			stdErrStr := string(e.Stderr)
			return &Result{
				Left: &LeftResult{
					Index: leftJoinCell,
					Row:   leftjoinRow,
				},
				Right: &RightResult{
					ExecResult: &ExecResult{
						ExecStdout: stdOutStr,
						ExecStdErr: stdErrStr,
						ExitCode:   code,
					},
				},
			}, nil
		} else {
			return nil, err
		}
	}

	exitCode := 0
	return &Result{
		Left: &LeftResult{
			Index: leftJoinCell,
			Row:   leftjoinRow,
		},
		Right: &RightResult{
			ExecResult: &ExecResult{
				ExecStdout: stdOutStr,
				ExitCode:   exitCode,
			},
		},
	}, nil
}

// joinData is expected to be a json string such as `{"foo": {"bar": {"baz": [0, 1, 2, 3, 4]}}}`
// query: "foo.bar.baz[2]"
// return value: "" or 2
//
// The data queried out ideally should be a string, or at least will be attempted to be cast to a
// a string for the stake of joining.
func searchJSONWithQuery(jsonData string, options QueryOptions) (string, error) {
	var data interface{}
	err := json.Unmarshal([]byte(jsonData), &data)
	if err != nil && options.AttemptToClean {
		// attempt a recovery to unescape things
		// since the CSV parser *sometimes* sends them in with double quotes
		attempt2 := doubleQuotes.ReplaceAllString(jsonData, `"`)
		err = json.Unmarshal([]byte(attempt2), &data)
		if err != nil {
			// not recoverable,
			return "", fmt.Errorf("failure to deserialize JSON, %v. Data %v", err, jsonData)
		}
	}
	if err != nil && !options.AttemptToClean {
		return "", fmt.Errorf("failure to deserialize JSON, %v. Data %v", err, jsonData)
	}

	result, err := jmespath.Search(options.JsonSubquery, data)
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

func attemptSplitAndSelectCol(row string, options QueryOptions) (string, error) {

	if strings.TrimSpace(row) == "" {
		return "", nil
	}

	if options.JoinColumn < 0 && options.JsonSubquery == "" {
		return row, nil // if we're joining on the whole row, don't bother splitting
	}
	if options.JoinColumn < 0 && options.JsonSubquery != "" {
		return searchJSONWithQuery(row, options)
	}
	var joinCell string

	// csv split using CSV parser
	if options.Separator == "," {
		// this a hack for nonstandard CSVs using slash quotes instead of double quotes for CSV
		// and is a very ugly way of solving the fact the golang CSV parser doesn't appear
		// to allow for any configurability here to provide alternate escapes
		if options.AttemptToClean {
			row = slashEscapeForQuotesRE.ReplaceAllString(row, `""`)
		}
		csvParser := csv.NewReader(strings.NewReader(row))
		csvParser.LazyQuotes = true
		res, err := csvParser.ReadAll()

		if err != nil {
			return "", fmt.Errorf("failure to parse CSV: %v. Data %v", err, row)
		}
		if len(res) < 1 {
			return "", fmt.Errorf("failure to parse CSV, couldn't find any rows to parse correctly")
		}
		if len(res) > 1 {
			return "", fmt.Errorf("failure to parse CSV, found more than a single row to parse")
		}
		if len(res[0])-1 < options.JoinColumn {
			return "", fmt.Errorf("failure to parse CSV and fetch column %v, only found %v columns. Data: %v",
				options.JoinColumn, len(res[0]), row)
		}
		joinCell = res[0][options.JoinColumn]
	} else { // not using CSV split, so just do a string split
		split := strings.Split(row, options.Separator)
		if len(split)-1 < options.JoinColumn {
			return "", fmt.Errorf("couldn't split row with separator %s and get '%v'th column. Only %d columns found. Remember this is zero-based index. \n\nRow contents: %s", options.Separator, options.JoinColumn, len(split), row)
		}
		joinCell = split[options.JoinColumn]
	}
	// no json involved, simple text case
	if options.JsonSubquery == "" {
		return strings.TrimSpace(joinCell), nil
	}

	return searchJSONWithQuery(joinCell, options)
}
