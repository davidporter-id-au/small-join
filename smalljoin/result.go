package smalljoin

import "encoding/json"

type LeftResult struct {
	Index string
	Row   string
}

type IndexFileResult struct {
	Index string
	Row   string
}

type ExecResult struct {
	ExecStdout string
	ExecStdErr string
	ExitCode   int
}

type RightResult struct {
	IndexFileResult *IndexFileResult
	ExecResult      *ExecResult
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
