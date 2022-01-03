package smalljoin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeftoverStringHandling(t *testing.T) {

	threeLines := `start - line 1 - end
start - line 2 - end
start - line 3 ...` // this line doesn't have a newline char

	continuation := `- end
start - line 4 - end
`

	tests := map[string]struct {
		prevRemainder     string
		input             []byte
		expectedBlock     []string
		expectedRemainder string
	}{
		"simple three lines, where the third isn't terminated": {
			prevRemainder: "",
			input:         []byte(threeLines),
			expectedBlock: []string{
				"start - line 1 - end",
				"start - line 2 - end",
			},
			expectedRemainder: "start - line 3 ...",
		},
		"simple lines with whitespace for complete lines, but not partials": {
			prevRemainder: "",
			input: []byte(`test 1   
				test 2
				test three...  `),
			expectedBlock: []string{
				"test 1",
				"test 2",
			},
			expectedRemainder: "\t\t\t\ttest three...  ", // don't remove whitespace, because this is a partial
		},
		"simple continuation": {
			prevRemainder: "start - line 3 ...",
			input:         []byte(continuation),
			expectedBlock: []string{
				"start - line 3 ...- end",
				"start - line 4 - end",
			},
			expectedRemainder: "",
		},
	}

	for name, td := range tests {
		out, remaining := splitInputBytes(td.prevRemainder, td.input)
		assert.Equal(t, td.expectedRemainder, remaining, name)
		assert.Equal(t, td.expectedBlock, out)
	}
}
