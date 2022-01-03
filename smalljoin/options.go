package smalljoin

const defaultConcurrency = 10
const defaultInputByteLen = 5000

type Jointype int

const (
	JoinTypeInner = iota
	JoinTypeLeft
	JoinTypeDisjoin
)

type QueryOptions struct {
	JsonSubquery string
	Separator    string
	JoinColumn   int
}

type Options struct {
	IncomingBufferSize int
	Concurrency        int
	IndexFile          string
	Jointype           Jointype
	continueOnErr      bool
	LeftQueryOptions   QueryOptions
	RightQueryOptions  QueryOptions
	OutputDebugMode    bool
}
