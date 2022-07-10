// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
    "blockwatch.cc/packdb/util"
    "blockwatch.cc/packdb/vec"
)

type ConditionTreeNode struct {
    OrKind   bool
    Children []ConditionTreeNode
    Cond     *Condition
}

func (n ConditionTreeNode) Empty() bool {
    return len(n.Children) == 0 && n.Cond == nil
}

func (n ConditionTreeNode) Leaf() bool {
    return n.Cond != nil
}

func (n ConditionTreeNode) NoMatch() bool {
    if n.Empty() {
        return false
    }

    if n.Leaf() {
        return n.Cond.nomatch
    }

    if n.OrKind {
        for _, v := range n.Children {
            if !v.NoMatch() {
                return false
            }
        }
        return true
    } else {
        for _, v := range n.Children {
            if v.NoMatch() {
                return true
            }
        }
        return false
    }
}

func (n ConditionTreeNode) Compile() error {
    if n.Leaf() {
        if err := n.Cond.Compile(); err != nil {
            return nil
        }
    } else {
        for _, v := range n.Children {
            if err := v.Compile(); err != nil {
                return err
            }
        }
    }
    return nil
}

func (n ConditionTreeNode) Fields() FieldList {
    if n.Empty() {
        return nil
    }
    if n.Leaf() {
        return FieldList{n.Cond.Field}
    }
    fl := make(FieldList, 0)
    for _, v := range n.Children {
        fl = fl.AddUnique(v.Fields()...)
    }
    return fl
}

func (n ConditionTreeNode) Size() int {
    if n.Leaf() {
        return 1
    }
    l := 0
    for _, v := range n.Children {
        l += v.Size()
    }
    return l
}

// Depth returns the max number of tree levels
func (n ConditionTreeNode) Depth() int {
    return n.depth(0)
}

func (n ConditionTreeNode) depth(level int) int {
    if n.Empty() {
        return level
    }
    if n.Leaf() {
        return level + 1
    }
    d := level + 1
    for _, v := range n.Children {
        d = util.Max(d, v.depth(level+1))
    }
    return d
}

func (n ConditionTreeNode) Weight() int {
    if n.Leaf() {
        return n.Cond.NValues()
    }
    w := 0
    for _, v := range n.Children {
        w += v.Weight()
    }
    return w
}

func (n ConditionTreeNode) Cost(info PackInfo) int {
    return n.Weight() * info.NValues
}

func (n ConditionTreeNode) Conditions() []*Condition {
    if n.Leaf() {
        return []*Condition{n.Cond}
    }
    cond := make([]*Condition, 0)
    for _, v := range n.Children {
        cond = append(cond, v.Conditions()...)
    }
    return cond
}

func (n *ConditionTreeNode) AddAndCondition(c *Condition) {
    node := ConditionTreeNode{
        OrKind: COND_AND,
        Cond:   c,
    }
    n.AddNode(node)
}

func (n *ConditionTreeNode) AddOrCondition(c *Condition) {
    node := ConditionTreeNode{
        OrKind: COND_OR,
        Cond:   c,
    }
    n.AddNode(node)
}

// Invariants
// - root is always and AND node
// - root is never a leaf node
// - root may be empty
func (n *ConditionTreeNode) AddNode(node ConditionTreeNode) {
    if n.Leaf() {
        clone := ConditionTreeNode{
            OrKind:   n.OrKind,
            Children: n.Children,
            Cond:     n.Cond,
        }
        n.Cond = nil
        n.Children = []ConditionTreeNode{clone}
    }

    // append new condition to this element
    if n.OrKind == node.OrKind && !node.Leaf() {
        n.Children = append(n.Children, node.Children...)
    } else {
        n.Children = append(n.Children, node)
    }
}

func (n ConditionTreeNode) MaybeMatchPack(info PackInfo) bool {
    if info.NValues == 0 {
        return false
    }

    if n.Empty() {
        return true
    }

    if n.Leaf() {
        return n.Cond.MaybeMatchPack(info)
    }

    for _, v := range n.Children {
        if n.OrKind {
            if v.MaybeMatchPack(info) {
                return true
            }
        } else {
            if !v.MaybeMatchPack(info) {
                return false
            }
        }
    }

    // no OR nodes match
    if n.OrKind {
        return false
    }
    // all AND nodes match
    return true
}

func (n ConditionTreeNode) MatchPack(pkg *Package, info PackInfo) *vec.BitSet {
    if n.Leaf() {
        return n.Cond.MatchPack(pkg, nil)
    }

    if n.Empty() {
        return vec.NewBitSet(pkg.Len()).One()
    }

    if n.OrKind {
        return n.MatchPackOr(pkg, info)
    } else {
        return n.MatchPackAnd(pkg, info)
    }
}

func (n ConditionTreeNode) MatchPackAnd(pkg *Package, info PackInfo) *vec.BitSet {
    bits := vec.NewBitSet(pkg.Len()).One()

    for _, cn := range n.Children {
        var b *vec.BitSet
        if !cn.Leaf() {
            b = cn.MatchPack(pkg, info)
        } else {
            c := cn.Cond
            if !pkg.IsJournal() && len(info.Blocks) > c.Field.Index {
                min, max := info.Blocks[c.Field.Index].MinValue, info.Blocks[c.Field.Index].MaxValue
                switch c.Mode {
                case FilterModeEqual:
                    if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
                        continue
                    }
                case FilterModeNotEqual:
                    if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
                        continue
                    }
                case FilterModeRange:
                    if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
                        continue
                    }
                case FilterModeGt:
                    if c.Field.Type.Gt(min, c.Value) {
                        continue
                    }
                case FilterModeGte:
                    if c.Field.Type.Gte(min, c.Value) {
                        continue
                    }
                case FilterModeLt:
                    if c.Field.Type.Lt(max, c.Value) {
                        continue
                    }
                case FilterModeLte:
                    if c.Field.Type.Lte(max, c.Value) {
                        continue
                    }
                }
            }
            b = c.MatchPack(pkg, bits)
        }

        if bits.Count() == int64(bits.Size()) {
            bits.Close()
            bits = b
            continue
        }

        bits.And(b)
        b.Close()

        if bits.Count() == 0 {
            break
        }
    }
    return bits
}

func (n ConditionTreeNode) MatchPackOr(pkg *Package, info PackInfo) *vec.BitSet {
    bits := vec.NewBitSet(pkg.Len())
    for _, cn := range n.Children {
        var b *vec.BitSet
        if !cn.Leaf() {
            b = cn.MatchPack(pkg, info)
        } else {
            c := cn.Cond
            if !pkg.IsJournal() && len(info.Blocks) > c.Field.Index {
                min, max := info.Blocks[c.Field.Index].MinValue, info.Blocks[c.Field.Index].MaxValue
                skipEarly := false
                switch c.Mode {
                case FilterModeEqual:
                    if c.Field.Type.Equal(min, c.Value) && c.Field.Type.Equal(max, c.Value) {
                        skipEarly = true
                    }
                case FilterModeNotEqual:
                    if c.Field.Type.Lt(c.Value, min) || c.Field.Type.Gt(c.Value, max) {
                        skipEarly = true
                    }
                case FilterModeRange:
                    if c.Field.Type.Lte(c.From, min) && c.Field.Type.Gte(c.To, max) {
                        skipEarly = true
                    }
                case FilterModeGt:
                    if c.Field.Type.Gt(min, c.Value) {
                        skipEarly = true
                    }
                case FilterModeGte:
                    if c.Field.Type.Gte(min, c.Value) {
                        skipEarly = true
                    }
                case FilterModeLt:
                    if c.Field.Type.Lt(max, c.Value) {
                        skipEarly = true
                    }
                case FilterModeLte:
                    if c.Field.Type.Lte(max, c.Value) {
                        skipEarly = true
                    }
                }
                if skipEarly {
                    bits.Close()
                    return vec.NewBitSet(pkg.Len()).One()
                }
            }
            b = c.MatchPack(pkg, bits)
        }

        bits.Or(b)
        b.Close()

        if bits.Count() == int64(bits.Size()) {
            break
        }
    }
    return bits
}

func (n ConditionTreeNode) MatchAt(pkg *Package, pos int) bool {
    if n.Leaf() {
        return n.Cond.MatchAt(pkg, pos)
    }

    if n.Empty() {
        return true
    }

    if n.OrKind {
        for _, c := range n.Children {
            if c.MatchAt(pkg, pos) {
                return true
            }
        }
    } else {
        for _, c := range n.Children {
            if !c.MatchAt(pkg, pos) {
                return false
            }
        }
    }
    return true
}
