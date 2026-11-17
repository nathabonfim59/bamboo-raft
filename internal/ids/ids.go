// Package ids makes the public identifiers. Sandbox ids match the API's
// pattern ^bx_[23456789abcdefghjkmnpqrstuvwxyz]{8}$ (no 0, 1, i, l, o).
package ids

import (
	"crypto/rand"
	"math/big"
	"strings"
)

// Alphabet holds the characters of an id's random part.
const Alphabet = "23456789abcdefghjkmnpqrstuvwxyz"

const sandboxPrefix = "bx_"

// New returns prefix followed by n random characters from Alphabet.
func New(prefix string, n int) string {
	var b strings.Builder
	b.Grow(len(prefix) + n)
	b.WriteString(prefix)
	max := big.NewInt(int64(len(Alphabet)))
	for range n {
		i, err := rand.Int(rand.Reader, max)
		if err != nil {
			panic(err) // crypto/rand never fails on Linux
		}
		b.WriteByte(Alphabet[i.Int64()])
	}
	return b.String()
}

// Sandbox returns a new sandbox id, e.g. bx_f7k2q9hd.
func Sandbox() string { return New(sandboxPrefix, 8) }

// ValidSandbox reports whether s has the shape of a sandbox id.
func ValidSandbox(s string) bool {
	rest, ok := strings.CutPrefix(s, sandboxPrefix)
	if !ok || len(rest) != 8 {
		return false
	}
	for i := range len(rest) {
		if strings.IndexByte(Alphabet, rest[i]) < 0 {
			return false
		}
	}
	return true
}
