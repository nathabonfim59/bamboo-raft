package overcommit

import (
	"errors"
	"math"
	"testing"
)

func TestScaleExact(t *testing.T) {
	const two53 = uint64(1) << 53
	for _, tc := range []struct {
		name  string
		base  uint64
		ratio float64
		want  uint64
	}{
		{"zero base", 0, 2, 0},
		{"ratio one", 12345, 1, 12345},
		{"ratio one max", math.MaxUint64, 1, math.MaxUint64},
		{"two", 21, 2, 42},
		{"half floors", 3, 0.5, 1},
		{"half below one", 1, 0.5, 0},
		{"fraction floors", 3, 1.5, 4},
		{"2^53 boundary", two53, 1, two53},
		{"2^53+1 exact", two53 + 1, 1, two53 + 1},
		{"2^53+1 doubled", two53 + 1, 2, two53*2 + 2},
		{"2^53 doubled", two53, 2, two53 * 2},
		{"max half", math.MaxUint64, 0.5, math.MaxUint64 / 2},
		{"decimal 1.1", 10, 1.1, 11},
		{"decimal 1.2", 10, 1.2, 12},
		{"decimal 0.3", 10, 0.3, 3},
		{"decimal 1.15", 100, 1.15, 115},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Scale(tc.base, tc.ratio)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("Scale(%d, %g) = %d, want %d", tc.base, tc.ratio, got, tc.want)
			}
		})
	}
}

func TestScaleRejectsInvalidRatio(t *testing.T) {
	for _, ratio := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := Scale(1, ratio); !errors.Is(err, ErrInvalidRatio) {
			t.Fatalf("Scale(1, %v) error = %v, want ErrInvalidRatio", ratio, err)
		}
		if _, err := Scale(0, ratio); !errors.Is(err, ErrInvalidRatio) {
			t.Fatalf("Scale(0, %v) error = %v, want ErrInvalidRatio", ratio, err)
		}
		if err := Validate(ratio); !errors.Is(err, ErrInvalidRatio) {
			t.Fatalf("Validate(%v) = %v, want ErrInvalidRatio", ratio, err)
		}
	}
	if err := Validate(1); err != nil {
		t.Fatalf("Validate(1) = %v", err)
	}
}

func TestScaleRejectsOverflow(t *testing.T) {
	for _, base := range []uint64{math.MaxUint64, math.MaxUint64/2 + 1, 1 << 63} {
		if _, err := Scale(base, 2); !errors.Is(err, ErrOverflow) {
			t.Fatalf("Scale(%d, 2) error = %v, want ErrOverflow", base, err)
		}
	}
}
