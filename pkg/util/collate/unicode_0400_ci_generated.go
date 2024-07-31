// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by "util/collate/ucaimpl"; DO NOT EDIT.
// These codes are generated rather than using other polymorphism method (e.g. generics, interfaces, if/else...) to make
// sure every call to the `GetWeight` and `Preprocess` is inlined. The function inlining can affect 20%~50% performance.

package collate

// unicodeCICollator implements UCA. see http://unicode.org/reports/tr10/
type unicodeCICollator struct {
	impl unicode0400Impl
}

// Clone implements Collator interface.
func (uc *unicodeCICollator) Clone() Collator {
    return &unicodeCICollator{impl: uc.impl.Clone()}
}

// Compare implements Collator interface.
func (uc *unicodeCICollator) Compare(a, b string) int {
	a = uc.impl.Preprocess(a)
	b = uc.impl.Preprocess(b)

	// weight of a, b. weight in unicode_ci may have 8 uint16s. xn indicate first 4 u16s, xs indicate last 4 u16s
	an, bn := uint64(0), uint64(0)
	as, bs := uint64(0), uint64(0)
	// rune of a, b
	ar, br := rune(0), rune(0)
	// decode index of a, b
	ai, bi := 0, 0
	for {
		if an == 0 {
			if as == 0 {
				for an == 0 && ai < len(a) {
					ar, ai = decodeRune(a, ai)
					an, as = uc.impl.GetWeight(ar)
				}
			} else {
				an = as
				as = 0
			}
		}

		if bn == 0 {
			if bs == 0 {
				for bn == 0 && bi < len(b) {
					br, bi = decodeRune(b, bi)
					bn, bs = uc.impl.GetWeight(br)
				}
			} else {
				bn = bs
				bs = 0
			}
		}

		if an == 0 || bn == 0 {
			return sign(int(an) - int(bn))
		}

		if an == bn {
			an, bn = 0, 0
			continue
		}

		for an != 0 && bn != 0 {
			if (an^bn)&0xFFFF != 0 {
				return sign(int(an&0xFFFF) - int(bn&0xFFFF))
			}
			an >>= 16
			bn >>= 16
		}
	}
}

// Key implements Collator interface.
func (uc *unicodeCICollator) Key(str string) []byte {
	return uc.KeyWithoutTrimRightSpace(uc.impl.Preprocess(str))
}

// KeyWithoutTrimRightSpace implements Collator interface.
func (uc *unicodeCICollator) KeyWithoutTrimRightSpace(str string) []byte {
	buf := make([]byte, 0, len(str)*2)
	r := rune(0)
	si := 0                        // decode index of s
	sn, ss := uint64(0), uint64(0) // weight of str. weight in unicode_ci may has 8 uint16s. sn indicate first 4 u16s, ss indicate last 4 u16s

	for si < len(str) {
		r, si = decodeRune(str, si)

		sn, ss = uc.impl.GetWeight(r)

		for sn != 0 {
			buf = append(buf, byte((sn&0xFF00)>>8), byte(sn))
			sn >>= 16
		}
		for ss != 0 {
			buf = append(buf, byte((ss&0xFF00)>>8), byte(ss))
			ss >>= 16
		}
	}
	return buf
}

// Pattern implements Collator interface.
func (uc *unicodeCICollator) Pattern() WildcardPattern {
	return uc.impl.Pattern()
}
