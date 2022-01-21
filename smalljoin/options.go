package smalljoin

const defaultConcurrency = 10
const defaultInputByteLen = 5000

type Jointype int

const (
	JoinTypeInner = iota
	JoinTypeLeft
	JoinTypeRightIsNull
)

type QueryOptions struct {
	JsonSubquery   string
	Separator      string
	JoinColumn     int
	AttemptToClean bool
}

type Options struct {
	IncomingBufferSize int
	Concurrency        int
	IndexFile          string
	RightExecStr       string
	Jointype           Jointype
	LeftQueryOptions   QueryOptions
	RightQueryOptions  QueryOptions
	ContinueOnErr      bool
	OutputDebugMode    bool
}

// the 'right' of the join is the index file
// and is intended to fit into memory map
// the key of the map is the join key, the
// data is the rest of the join row.
type rightIndex map[string]indexEntry

type indexEntry struct {
	data      string
	joinCount int32
}
