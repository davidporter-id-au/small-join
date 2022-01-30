package smalljoin

import "encoding/json"

// 'Left' is the streaming side input value which was attempted to be
// matched and its indexes.
//
// 'Row' is the entire contents of the row from the matched result
// index is the thing that was attempted to be matched on
// eg: Given a successful match on this contents:
// "a, b, c"
// assuming that the index key is the second column
// then the entire 'row' contentents are "a, b, c"
// and the "index" is "b"
type LeftResult struct {
	Index string
	Row   string
}

// Right is either the input side or whatever side that's being
// checked against for the stream. Its either going to be a small file of
// indexes or maybe some eval result or whatever.
type RightResult struct {
	IndexFileResult *IndexFileResult `json:"IndexFileResult,omitempty"`
	ExecResult      *ExecResult      `json:"ExecResult,omitempty"`
}

type IndexFileResult struct {
	Index string // index is the thign that was attempted to be matched on
	Row   string // Row is the entire contents of the row from the matched result
}

type ExecResult struct {
	ExecStdout string
	ExecStdErr string
	ExitCode   int
}

type Result struct {
	Left  *LeftResult
	Right *RightResult
}

func (r Result) String() string {
	d, _ := json.Marshal(r)
	return string(d)
}

func (r Result) SuccessfulJoin(joinType Jointype) bool {
	switch joinType {
	case JoinTypeLeft:
		return r.Left != nil
	case JoinTypeRightIsNull:
		if r.Left == nil {
			return false
		}
		if r.Right == nil {
			return true
		}
		if r.Right.IndexFileResult != nil {
			return false
		}
		if r.Right.ExecResult != nil {
			return r.Right.ExecResult.ExitCode != 0
		}
	case JoinTypeInner:
		if r.Left == nil || r.Right == nil {
			return false
		}
		if r.Right.IndexFileResult != nil {
			return true
		}
		if r.Right.ExecResult != nil && r.Right.ExecResult.ExitCode == 0 {
			return true
		}
		return false
	}
	panic("Invalid join spec specified")
}
