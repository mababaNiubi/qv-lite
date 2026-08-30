package tsdb

import (
	"runtime"
	"testing"

	"github.com/mababaNiubi/variant"
)

// 对比 QueryWindow avg 的两条实现路径（1M 点窗口聚合循环）：
// 快路径 = 手写 int64 算术（QueryWindow 内联版）；
// 泛型路径 = variant.Reduce/Divide/Increase 三调用链（UInt64/混合类型路径）。

func BenchmarkWindowAvgFastPath(b *testing.B) {
	target := variant.NewInt64(0)
	varCount := 0
	for i := 0; i < b.N; i++ {
		v := variant.NewInt64(int64(i))
		varCount++
		ti, _ := target.AsInt64()
		vi, _ := v.AsInt64()
		target = variant.NewInt64(ti + (vi-ti)/int64(varCount))
	}
	runtime.KeepAlive(target)
}

func BenchmarkWindowAvgGeneric(b *testing.B) {
	target := variant.NewInt64(0)
	varCount := 0
	for i := 0; i < b.N; i++ {
		v := variant.NewInt64(int64(i))
		varCount++
		reduceVariant, err := v.Reduce(target)
		if err != nil {
			b.Fatal(err)
		}
		divideValue, err := reduceVariant.Divide(variant.NewInt64(int64(varCount)))
		if err != nil {
			b.Fatal(err)
		}
		target, err = target.Increase(divideValue)
		if err != nil {
			b.Fatal(err)
		}
	}
	runtime.KeepAlive(target)
}

func BenchmarkWindowAvgFastPathFloat(b *testing.B) {
	target := variant.NewFloat64(0)
	varCount := 0
	for i := 0; i < b.N; i++ {
		v := variant.NewFloat64(float64(i))
		varCount++
		tf, _ := target.AsFloat64()
		vf, _ := v.AsFloat64()
		target = variant.NewFloat64(tf + (vf-tf)/float64(varCount))
	}
	runtime.KeepAlive(target)
}

func BenchmarkWindowAvgGenericFloat(b *testing.B) {
	target := variant.NewFloat64(0)
	varCount := 0
	for i := 0; i < b.N; i++ {
		v := variant.NewFloat64(float64(i))
		varCount++
		reduceVariant, err := v.Reduce(target)
		if err != nil {
			b.Fatal(err)
		}
		divideValue, err := reduceVariant.Divide(variant.NewFloat64(float64(varCount)))
		if err != nil {
			b.Fatal(err)
		}
		target, err = target.Increase(divideValue)
		if err != nil {
			b.Fatal(err)
		}
	}
	runtime.KeepAlive(target)
}
