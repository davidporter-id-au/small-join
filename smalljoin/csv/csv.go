package smalljoin

import (
	"fmt"
	"regexp"
	"strings"
)

const csvQuoteChar = '"' // the way in which this program handles a quote string
const columnBreak = ","
const csvEscapeChar = '\\' // the way in which a character is escaped
var escapedQuoteRE *regexp.Regexp
var escapedQuotesRE *regexp.Regexp

func init() {
	escapedQuoteRE = regexp.MustCompile(`\"`)
	escapedQuoteRE = regexp.MustCompile(`("")|(".*?[^\\]")`)
}

type csvBlock struct {
	s        string
	isQuoted bool // this is a quoted CSV, don't split it
}

// BreakCSVIntoColumns does exactly what it sounds like, it attempts to
// handle the nasty edge-cases of CSV parsing and escaping by breaking things it columns:
//
// EG:
// - finding quoted values and breaking them down
// - handling commas inside quoted values
//
// input: `11603FEA-FB08-4E17-A037-74F1975B02E5,11603FEA-FB08-4E17-A037-74F1975B02E5,,"{\"data1\":1232,\"data2\":\"\"}","{\"data 3\":[1, 2, 3]}","another string"`,
// expectedValue: []string{
// 	"11603FEA-FB08-4E17-A037-74F1975B02E5",
// 	"11603FEA-FB08-4E17-A037-74F1975B02E5",
// 	"",
// 	`{"data1":1232,"data2":""}`,
// 	`{"data 3":[1, 2, 3]}`,
// },
//
// This is, I'm sure, some first-year undergrad parsing problem
// for which I'm ill prepared, never having done parsers, so please treat it as hacky,
// and very likely fairly wrong in parts
func BreakCSVIntoColumns(in string) ([]string, error) {
	var out []string
	inputRunes := []rune(in)
	quoteBreaks, err := findQuoteBreaks(inputRunes)
	if err != nil {
		return nil, err
	}
	unescaped := unescapeQuotedParts(quoteBreaks)
	commaSplit := splitByCommas(unescaped)
	for c := range commaSplit {
		out = append(out, commaSplit[c].s)
	}
	return out, nil
}

func findQuoteBreaks(in []rune) ([]csvBlock, error) {
	var out []csvBlock
	b := strings.Builder{}
	// determine if we're starting out with something quoted or not
	parsingQuoteBlock := in[0] == csvQuoteChar
	// loop over all the characters in the string
	for i := range in {
		// and if you hit any subsequent quote, which doesn't have a prior escape character
		// then flip the quoteblock switch and call it a block
		if in[i] == csvQuoteChar && (i-1 > 0 && in[i-1] != csvEscapeChar) {
			out = append(out, csvBlock{
				s:        b.String(),
				isQuoted: parsingQuoteBlock,
			})
			parsingQuoteBlock = !parsingQuoteBlock
			b.Reset()
		} else if in[i] == csvQuoteChar && i == 0 {
			// annoying special case, the first char is a quote
			// just skip, already capturing with the parsingQuoteBlock bool
		} else {
			b.WriteRune(in[i])
		}
	}
	if parsingQuoteBlock {
		return nil, fmt.Errorf("found unmatched quote, something is wrong with the line: %s", string(in))
	}
	if b.Len() != 0 {
		out = append(out, csvBlock{
			s:        b.String(),
			isQuoted: parsingQuoteBlock,
		})
	}
	return out, nil
}

func unescapeQuotedParts(in []csvBlock) []csvBlock {
	for i := range in {
		if in[i].isQuoted {
			in[i].s = escapedQuoteRE.ReplaceAllString(in[i].s, `"`)
		}
	}
	return in
}

func splitByCommas(in []csvBlock) []csvBlock {
	var out []csvBlock
	for i := range in {
		if in[i].isQuoted {
			out = append(out, in[i])
		} else {
			csvSplit := strings.Split(in[i].s, columnBreak)
			for j := range csvSplit {
				out = append(out, csvBlock{s: csvSplit[j]})
			}
		}
	}
	return out
}
