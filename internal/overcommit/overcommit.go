// Package overcommit contains the exact checked arithmetic shared by the node
// command's capacity resolution and the manager's live admission limits.
package overcommit

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
)

// ErrInvalidRatio reports a ratio that is not a finite positive number.
var ErrInvalidRatio = errors.New("overcommit: ratio must be finite and positive")

// ErrOverflow reports a scaled result that does not fit in uint64.
var ErrOverflow = errors.New("overcommit: scaled value overflows uint64")

// Validate reports whether ratio is usable: finite and strictly positive.
func Validate(ratio float64) error {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio <= 0 {
		return ErrInvalidRatio
	}
	return nil
}

// Scale returns floor(base*ratio) with exact rational arithmetic. The float64
// ratio is first rendered as its shortest round-trip decimal, which is then
// parsed as an exact rational, so configured decimal ratios such as 1.2 and
// 0.3 behave as written instead of using their binary approximation. Results
// at the 2^53 and uint64 boundaries do not suffer multiplication rounding, and
// the function never returns a wrapped or silently clamped value.
func Scale(base uint64, ratio float64) (uint64, error) {
	if err := Validate(ratio); err != nil {
		return 0, err
	}
	if base == 0 {
		return 0, nil
	}
	decimal := strconv.FormatFloat(ratio, 'g', -1, 64)
	r, ok := new(big.Rat).SetString(decimal)
	if !ok {
		return 0, ErrInvalidRatio
	}
	product := new(big.Int).Mul(new(big.Int).SetUint64(base), r.Num())
	product.Quo(product, r.Denom())
	if !product.IsUint64() {
		return 0, fmt.Errorf("%w: %d * %g", ErrOverflow, base, ratio)
	}
	return product.Uint64(), nil
}
