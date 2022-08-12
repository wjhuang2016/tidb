// Copyright 2019 PingCAP, Inc.
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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinAddDatetimeAndDurationSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.Times()

	arg1s := buf1.GoDurations()

	resultSlice := result.Times()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := arg1s[i]

		// calculate

		output, err := arg0.Add(b.ctx.GetSessionVars().StmtCtx, types.Duration{Duration: arg1, Fsp: -1})

		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinAddDatetimeAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinAddDatetimeAndStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.Times()

	resultSlice := result.Times()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := buf1.GetString(i)

		// calculate

		if !isDuration(arg1) {
			result.SetNull(i, true) // fixed: true
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.SetNull(i, true) // fixed: true
				continue
			}
			return err
		}

		output, err := arg0.Add(sc, arg1Duration)

		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinAddDatetimeAndStringSig) vectorized() bool {
	return true
}

func (b *builtinAddDurationAndDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalDuration(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.GoDurations()

	arg1s := buf1.GoDurations()

	resultSlice := result.GoDurations()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := arg1s[i]

		// calculate

		output, err := types.AddDuration(arg0, arg1)
		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinAddDurationAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinAddDurationAndStringSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalDuration(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.GoDurations()

	resultSlice := result.GoDurations()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := buf1.GetString(i)

		// calculate

		if !isDuration(arg1) {
			result.SetNull(i, true) // fixed: true
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.SetNull(i, true) // fixed: true
				continue
			}
			return err
		}

		output, err := types.AddDuration(arg0, arg1Duration.Duration)
		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinAddDurationAndStringSig) vectorized() bool {
	return true
}

func (b *builtinAddStringAndDurationSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	arg1s := buf1.GoDurations()

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := buf0.GetString(i)

		arg1 := arg1s[i]

		// calculate

		sc := b.ctx.GetSessionVars().StmtCtx
		fsp1 := b.args[1].GetType().GetDecimal()
		arg1Duration := types.Duration{Duration: arg1, Fsp: fsp1}
		var output string
		var isNull bool
		if isDuration(arg0) {

			output, err = strDurationAddDuration(sc, arg0, arg1Duration)

			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					result.AppendNull() // fixed: false
					continue
				}
				return err
			}
		} else {

			output, isNull, err = strDatetimeAddDuration(sc, arg0, arg1Duration)

			if err != nil {
				return err
			}
			if isNull {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
		}

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinAddStringAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinAddStringAndStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}

	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.GetFlag()) {
		result.ReserveString(n)
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
		return nil
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := buf0.GetString(i)

		arg1 := buf1.GetString(i)

		// calculate

		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, getFsp4TimeAddSub(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
			return err
		}

		var output string
		var isNull bool
		if isDuration(arg0) {

			output, err = strDurationAddDuration(sc, arg0, arg1Duration)

			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					result.AppendNull() // fixed: false
					continue
				}
				return err
			}
		} else {

			output, isNull, err = strDatetimeAddDuration(sc, arg0, arg1Duration)

			if err != nil {
				return err
			}
			if isNull {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
		}

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinAddStringAndStringSig) vectorized() bool {
	return true
}

func (b *builtinAddDateAndDurationSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	arg0s := buf0.GoDurations()

	arg1s := buf1.GoDurations()

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := arg1s[i]

		// calculate

		fsp0 := b.args[0].GetType().GetDecimal()
		fsp1 := b.args[1].GetType().GetDecimal()
		arg1Duration := types.Duration{Duration: arg1, Fsp: fsp1}

		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Add(arg1Duration)

		if err != nil {
			return err
		}
		output := sum.String()

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinAddDateAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinAddDateAndStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	arg0s := buf0.GoDurations()

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := buf1.GetString(i)

		// calculate

		if !isDuration(arg1) {
			result.AppendNull() // fixed: false
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, getFsp4TimeAddSub(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
			return err
		}

		fsp0 := b.args[0].GetType().GetDecimal()

		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Add(arg1Duration)

		if err != nil {
			return err
		}
		output := sum.String()

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinAddDateAndStringSig) vectorized() bool {
	return true
}

func (b *builtinAddTimeDateTimeNullSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ResizeTime(n, true)

	return nil
}

func (b *builtinAddTimeDateTimeNullSig) vectorized() bool {
	return true
}

func (b *builtinAddTimeStringNullSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendNull()
	}

	return nil
}

func (b *builtinAddTimeStringNullSig) vectorized() bool {
	return true
}

func (b *builtinAddTimeDurationNullSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ResizeGoDuration(n, true)

	return nil
}

func (b *builtinAddTimeDurationNullSig) vectorized() bool {
	return true
}

func (b *builtinSubDatetimeAndDurationSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.Times()

	arg1s := buf1.GoDurations()

	resultSlice := result.Times()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := arg1s[i]

		// calculate

		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration := types.Duration{Duration: arg1, Fsp: -1}
		output, err := arg0.Add(sc, arg1Duration.Neg())

		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinSubDatetimeAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinSubDatetimeAndStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.Times()

	resultSlice := result.Times()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := buf1.GetString(i)

		// calculate

		if !isDuration(arg1) {
			result.SetNull(i, true) // fixed: true
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.SetNull(i, true) // fixed: true
				continue
			}
			return err
		}
		output, err := arg0.Add(sc, arg1Duration.Neg())

		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinSubDatetimeAndStringSig) vectorized() bool {
	return true
}

func (b *builtinSubDurationAndDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalDuration(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.GoDurations()

	arg1s := buf1.GoDurations()

	resultSlice := result.GoDurations()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := arg1s[i]

		// calculate

		output, err := types.SubDuration(arg0, arg1)
		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinSubDurationAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinSubDurationAndStringSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalDuration(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)

	arg0s := buf0.GoDurations()

	resultSlice := result.GoDurations()

	for i := 0; i < n; i++ {

		if result.IsNull(i) {
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := buf1.GetString(i)

		// calculate

		if !isDuration(arg1) {
			result.SetNull(i, true) // fixed: true
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.SetNull(i, true) // fixed: true
				continue
			}
			return err
		}

		output, err := types.SubDuration(arg0, arg1Duration.Duration)
		if err != nil {
			return err
		}

		// commit result

		resultSlice[i] = output

	}
	return nil
}

func (b *builtinSubDurationAndStringSig) vectorized() bool {
	return true
}

func (b *builtinSubStringAndDurationSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	arg1s := buf1.GoDurations()

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := buf0.GetString(i)

		arg1 := arg1s[i]

		// calculate

		sc := b.ctx.GetSessionVars().StmtCtx
		fsp1 := b.args[1].GetType().GetDecimal()
		arg1Duration := types.Duration{Duration: arg1, Fsp: fsp1}
		var output string
		var isNull bool
		if isDuration(arg0) {

			output, err = strDurationSubDuration(sc, arg0, arg1Duration)

			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					result.AppendNull() // fixed: false
					continue
				}
				return err
			}
		} else {

			output, isNull, err = strDatetimeSubDuration(sc, arg0, arg1Duration)

			if err != nil {
				return err
			}
			if isNull {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
		}

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinSubStringAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinSubStringAndStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}

	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.GetFlag()) {
		result.ReserveString(n)
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
		return nil
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := buf0.GetString(i)

		arg1 := buf1.GetString(i)

		// calculate

		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, getFsp4TimeAddSub(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
			return err
		}

		var output string
		var isNull bool
		if isDuration(arg0) {

			output, err = strDurationSubDuration(sc, arg0, arg1Duration)

			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					result.AppendNull() // fixed: false
					continue
				}
				return err
			}
		} else {

			output, isNull, err = strDatetimeSubDuration(sc, arg0, arg1Duration)

			if err != nil {
				return err
			}
			if isNull {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
		}

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinSubStringAndStringSig) vectorized() bool {
	return true
}

func (b *builtinSubDateAndDurationSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	arg0s := buf0.GoDurations()

	arg1s := buf1.GoDurations()

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := arg1s[i]

		// calculate

		fsp0 := b.args[0].GetType().GetDecimal()
		fsp1 := b.args[1].GetType().GetDecimal()
		arg1Duration := types.Duration{Duration: arg1, Fsp: fsp1}

		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Sub(arg1Duration)

		if err != nil {
			return err
		}
		output := sum.String()

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinSubDateAndDurationSig) vectorized() bool {
	return true
}

func (b *builtinSubDateAndStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)

	arg0s := buf0.GoDurations()

	for i := 0; i < n; i++ {

		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		// get arg0 & arg1

		arg0 := arg0s[i]

		arg1 := buf1.GetString(i)

		// calculate

		if !isDuration(arg1) {
			result.AppendNull() // fixed: false
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, _, err := types.ParseDuration(sc, arg1, getFsp4TimeAddSub(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.AppendNull() // fixed: false
				continue
			}
			return err
		}

		fsp0 := b.args[0].GetType().GetDecimal()

		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Sub(arg1Duration)

		if err != nil {
			return err
		}
		output := sum.String()

		// commit result

		result.AppendString(output)

	}
	return nil
}

func (b *builtinSubDateAndStringSig) vectorized() bool {
	return true
}

func (b *builtinSubTimeDateTimeNullSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ResizeTime(n, true)

	return nil
}

func (b *builtinSubTimeDateTimeNullSig) vectorized() bool {
	return true
}

func (b *builtinSubTimeStringNullSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendNull()
	}

	return nil
}

func (b *builtinSubTimeStringNullSig) vectorized() bool {
	return true
}

func (b *builtinSubTimeDurationNullSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ResizeGoDuration(n, true)

	return nil
}

func (b *builtinSubTimeDurationNullSig) vectorized() bool {
	return true
}

func (b *builtinNullTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, true)
	return nil
}

func (b *builtinNullTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinTimeStringTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalTime(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf0, buf1)
	arg0 := buf0.Times()
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lhsTime := arg0[i]
		_, rhsTime, rhsIsDuration, err := convertStringToDuration(stmtCtx, buf1.GetString(i), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if rhsIsDuration {
			result.SetNull(i, true)
			continue
		}
		d, isNull, err := calculateTimeDiff(stmtCtx, lhsTime, rhsTime)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinTimeStringTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinDurationStringTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf0 := result
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)
	arg0 := buf0.GoDurations()
	var (
		lhs types.Duration
		rhs types.Duration
	)
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lhs.Duration = arg0[i]
		rhsDur, _, rhsIsDuration, err := convertStringToDuration(stmtCtx, buf1.GetString(i), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if !rhsIsDuration {
			result.SetNull(i, true)
			continue
		}
		rhs = rhsDur
		d, isNull, err := calculateDurationTimeDiff(b.ctx, lhs, rhs)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinDurationStringTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinDurationDurationTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf0 := result
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalDuration(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)
	arg0 := buf0.GoDurations()
	arg1 := buf1.GoDurations()
	var (
		lhs types.Duration
		rhs types.Duration
	)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lhs.Duration = arg0[i]
		rhs.Duration = arg1[i]
		d, isNull, err := calculateDurationTimeDiff(b.ctx, lhs, rhs)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinDurationDurationTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinStringTimeTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalTime(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf0, buf1)
	arg1 := buf1.Times()
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		_, lhsTime, lhsIsDuration, err := convertStringToDuration(stmtCtx, buf0.GetString(i), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if lhsIsDuration {
			result.SetNull(i, true)
			continue
		}
		rhsTime := arg1[i]
		d, isNull, err := calculateTimeDiff(stmtCtx, lhsTime, rhsTime)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinStringTimeTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinStringDurationTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf1 := result
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)

	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalDuration(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf0)
	arg1 := buf1.GoDurations()
	var (
		lhs types.Duration
		rhs types.Duration
	)
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lhsDur, _, lhsIsDuration, err := convertStringToDuration(stmtCtx, buf0.GetString(i), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if !lhsIsDuration {
			result.SetNull(i, true)
			continue
		}
		lhs = lhsDur
		rhs.Duration = arg1[i]
		d, isNull, err := calculateDurationTimeDiff(b.ctx, lhs, rhs)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinStringDurationTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinStringStringTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf0, buf1)
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lhsDur, lhsTime, lhsIsDuration, err := convertStringToDuration(stmtCtx, buf0.GetString(i), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		rhsDur, rhsTime, rhsIsDuration, err := convertStringToDuration(stmtCtx, buf1.GetString(i), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if lhsIsDuration != rhsIsDuration {
			result.SetNull(i, true)
			continue
		}
		var (
			d      types.Duration
			isNull bool
		)
		if lhsIsDuration {
			d, isNull, err = calculateDurationTimeDiff(b.ctx, lhsDur, rhsDur)
		} else {
			d, isNull, err = calculateTimeDiff(stmtCtx, lhsTime, rhsTime)
		}
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinStringStringTimeDiffSig) vectorized() bool {
	return true
}

func (b *builtinTimeTimeTimeDiffSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	r64s := result.GoDurations()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalTime(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalTime(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf0, buf1)
	arg0 := buf0.Times()
	arg1 := buf1.Times()
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lhsTime := arg0[i]
		rhsTime := arg1[i]
		d, isNull, err := calculateTimeDiff(stmtCtx, lhsTime, rhsTime)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	return nil
}

func (b *builtinTimeTimeTimeDiffSig) vectorized() bool {
	return true
}
