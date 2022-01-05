package csv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBreakingUpCSVColumns(t *testing.T) {

	tests := map[string]struct {
		input         string
		jsonQuery     string
		expectedValue []string
		expectedErr   error
	}{
		"simple cases 1": {
			input: `a,a`,
			expectedValue: []string{
				`a`,
				`a`,
			},
		},
		"simple cases 2": {
			input: `a,a,,`,
			expectedValue: []string{
				`a`,
				`a`,
				``,
				``,
			},
		},
		"A quoted line": {
			input: `"{\"data1\":1232,\"data2\":\"\"}"`,
			expectedValue: []string{
				`{"data1":1232,"data2":""}`,
			},
		},
		"A quoted line with csvs": {
			input: `"{\"data1\":1232,\"data2\":\"\"}","asdf"`,
			expectedValue: []string{
				`{"data1":1232,"data2":""}`,
				`asdf`,
			},
		},
		"a valid CSV line": {
			input: `11603FEA-FB08-4E17-A037-74F1975B02E5,11603FEA-FB08-4E17-A037-74F1975B02E5,,"{\"data1\":1232,\"data2\":\"\"}","{\"data 3\":[1, 2, 3]}","another string"`,
			expectedValue: []string{
				"11603FEA-FB08-4E17-A037-74F1975B02E5",
				"11603FEA-FB08-4E17-A037-74F1975B02E5",
				"",
				`{"data1":1232,"data2":""}`,
				`{"data 3":[1, 2, 3]}`,
				`another string`,
			},
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := BreakCSVIntoColumns(td.input)
			assert.Equal(t, td.expectedValue, res, name)
			assert.Equal(t, td.expectedErr, err, name)
		})
	}
}

func TestFindQuoteBreaks(t *testing.T) {

	tests := map[string]struct {
		input         []rune
		jsonQuery     string
		expectedValue []csvBlock
		expectedErr   error
	}{
		"simple start": {
			input: []rune(`1232, "test , "`),
			expectedValue: []csvBlock{
				{s: `1232, `, isQuoted: false},
				{s: `test , `, isQuoted: true},
			},
		},
		"simple start 2": {
			input: []rune(`"test ,",1232`),
			expectedValue: []csvBlock{
				{s: `test ,`, isQuoted: true},
				{s: `,1232`, isQuoted: false},
			},
		},
		"a valid CSV line": {
			input: []rune(`11603FEA-FB08-4E17-A037-74F1975B02E5,11603FEA-FB08-4E17-A037-74F1975B02E5,,"{\"data1\":1232,\"data2\":\"\"}","{\"data 3\":[1, 2, 3]}","another string"`),
			expectedValue: []csvBlock{
				{s: "11603FEA-FB08-4E17-A037-74F1975B02E5,11603FEA-FB08-4E17-A037-74F1975B02E5,,"},
				{s: `{\"data1\":1232,\"data2\":\"\"}`, isQuoted: true},
				{s: `,`},
				{s: `{\"data 3\":[1, 2, 3]}`, isQuoted: true},
				{s: `,`},
				{s: `another string`, isQuoted: true},
			},
		},
	}

	for name, td := range tests {
		t.Run(name, func(t *testing.T) {
			res, err := findQuoteBreaks(td.input)
			assert.Equal(t, td.expectedValue, res, name)
			assert.Equal(t, td.expectedErr, err, name)
		})
	}
}
