package main

import (
	"flag"
	"strings"

	"log"
	"os"

	"github.com/davidporter-id-au/small-join/smalljoin"
	smallJoin "github.com/davidporter-id-au/small-join/smalljoin"
)

func main() {

	var joinStr string
	var join smallJoin.Jointype
	var right string

	var lSeparator string
	var lJsonSubquery string
	var lJoinColumn int

	var rSeparator string
	var rJsonSubquery string
	var rJoinColumn int
	var debugMode bool

	flag.StringVar(&right, "right", "", "the right side of the join file with the incoming stream, ie the indexes to read in")
	flag.StringVar(&joinStr, "join", "inner", "options: [inner|left|disjoint] The 'sql' type of join to apply on the two data streams")
	flag.BoolVar(&debugMode, "verbose", false, "output debug information")

	flag.StringVar(&lSeparator, "left-separator", ",", "a separator for the incoming stream")
	flag.StringVar(&lJsonSubquery, "left-json-subquery", "", "the JMES path to query and do a join on")
	flag.IntVar(&lJoinColumn, "left-join-column", -1, "the column number with which to attempt to join on. -1 imples there's no columns and to join on the entire row")

	flag.StringVar(&rSeparator, "right-separator", "", "a separator for the index file's columns with which to split it (eg, a comman for CSVs)")
	flag.StringVar(&rJsonSubquery, "right-json-subquery", "", "the JMES path to query and do a join on (if the contents of the column are JSON)")
	flag.IntVar(&rJoinColumn, "right-column", -1, "the column number with which to attempt to join on if there's a need to join only on a single column. \n-1 implies there's no clumns and join on the entire row")

	flag.Parse()

	s, err := os.Stat(right)
	if err != nil {
		log.Fatalf("Could not read right join file: %v, file: %v", err, right)
	}
	if s.IsDir() {
		log.Fatalf("not a valid file to join on")
	}

	switch strings.ToLower(joinStr) {
	case "inner":
		join = smalljoin.JoinTypeInner
	case "left":
		join = smalljoin.JoinTypeLeft
	default:
		log.Fatalf("not a valid join %q, options are: 'inner', 'left'\n", joinStr)
	}

	joiner := smallJoin.New(
		os.Stdin,
		os.Stdout,
		os.Stderr,
		smallJoin.Options{
			IndexFile:       right,
			Jointype:        join,
			OutputDebugMode: debugMode,
			LeftQueryOptions: smallJoin.QueryOptions{
				JoinColumn:   lJoinColumn,
				Separator:    lSeparator,
				JsonSubquery: lJsonSubquery,
			},
			RightQueryOptions: smallJoin.QueryOptions{
				JoinColumn:   rJoinColumn,
				Separator:    rSeparator,
				JsonSubquery: rJsonSubquery,
			},
		})

	err = joiner.Run()
	if err != nil {
		log.Fatalf("Fatal error while trying to join: %s", err)
	}
}
