package main

import (
	"github.com/codesoap/pbf-reblob/pbfproto"
)

var newStrings map[string]int

func merge(a, b *pbfproto.PrimitiveBlock) bool {
	if a.DateGranularity != nil && b.DateGranularity == nil ||
		a.DateGranularity == nil && b.DateGranularity != nil ||
		a.DateGranularity != nil && *a.DateGranularity != *b.DateGranularity ||
		a.LatOffset != nil && b.LatOffset == nil ||
		a.LatOffset == nil && b.LatOffset != nil ||
		a.LatOffset != nil && *a.LatOffset != *b.LatOffset ||
		a.LonOffset != nil && b.LonOffset == nil ||
		a.LonOffset == nil && b.LonOffset != nil ||
		a.LonOffset != nil && *a.LonOffset != *b.LonOffset ||
		a.Granularity != nil && b.Granularity == nil ||
		a.Granularity == nil && b.Granularity != nil ||
		a.Granularity != nil && *a.Granularity != *b.Granularity {
		// TODO: Transform instead of aborting.
		return false
	}
	if newStrings == nil {
		newStrings = make(map[string]int, len(a.Stringtable.S))
		for i, s := range a.Stringtable.S {
			newStrings[string(s)] = i
		}
	}
	i := len(a.Stringtable.S)
	for _, s := range b.Stringtable.S {
		if _, ok := newStrings[string(s)]; !ok {
			a.Stringtable.S = append(a.Stringtable.S, s)
			newStrings[string(s)] = i
			i++
		}
	}
	updateStringIndexes(b, newStrings)
	a.Primitivegroup = append(a.Primitivegroup, b.Primitivegroup...)
	return true
}

func updateStringIndexes(block *pbfproto.PrimitiveBlock, indexes map[string]int) {
	for _, group := range block.Primitivegroup {
		if len(group.Nodes) > 0 {
			updateNodesStringIndexes(group.Nodes, block.Stringtable.S, indexes)
		}
		if group.Dense != nil {
			updateDenseNodesStringIndexes(group.Dense, block.Stringtable.S, indexes)
		}
		if len(group.Ways) > 0 {
			updateWaysStringIndexes(group.Ways, block.Stringtable.S, indexes)
		}
		if len(group.Relations) > 0 {
			updateRelationsStringIndexes(group.Relations, block.Stringtable.S, indexes)
		}
	}
}

func updateNodesStringIndexes(nodes []*pbfproto.Node, oldStringtable [][]byte, indexes map[string]int) {
	for _, node := range nodes {
		if node.Info != nil && node.Info.UserSid != nil {
			newIndex := uint32(indexes[string(oldStringtable[*node.Info.UserSid])])
			node.Info.UserSid = &newIndex
		}
		newKeys := make([]uint32, len(node.Keys))
		for i, sid := range node.Keys {
			newKeys[i] = uint32(indexes[string(oldStringtable[sid])])
		}
		node.Keys = newKeys
		newVals := make([]uint32, len(node.Vals))
		for i, sid := range node.Vals {
			newVals[i] = uint32(indexes[string(oldStringtable[sid])])
		}
		node.Vals = newVals
	}
}

func updateDenseNodesStringIndexes(nodes *pbfproto.DenseNodes, oldStringtable [][]byte, indexes map[string]int) {
	if nodes.Denseinfo != nil {
		newSIDs := make([]int32, len(nodes.Denseinfo.UserSid))
		var sid int32
		for i, delta := range nodes.Denseinfo.UserSid {
			sid += delta
			if i == 0 {
				newSIDs[i] = int32(indexes[string(oldStringtable[sid])])
			} else {
				newSIDs[i] = int32(indexes[string(oldStringtable[sid])]) - newSIDs[i-1]
			}
		}
		nodes.Denseinfo.UserSid = newSIDs
	}
	newKeyVals := make([]int32, len(nodes.KeysVals))
	for i, sid := range nodes.KeysVals {
		if sid == 0 {
			continue
		}
		newKeyVals[i] = int32(indexes[string(oldStringtable[sid])])
	}
	nodes.KeysVals = newKeyVals
}

func updateWaysStringIndexes(ways []*pbfproto.Way, oldStringtable [][]byte, indexes map[string]int) {
	for _, way := range ways {
		if way.Info != nil && way.Info.UserSid != nil {
			newIndex := uint32(indexes[string(oldStringtable[*way.Info.UserSid])])
			way.Info.UserSid = &newIndex
		}
		newKeys := make([]uint32, len(way.Keys))
		for i, sid := range way.Keys {
			newKeys[i] = uint32(indexes[string(oldStringtable[sid])])
		}
		way.Keys = newKeys
		newVals := make([]uint32, len(way.Vals))
		for i, sid := range way.Vals {
			newVals[i] = uint32(indexes[string(oldStringtable[sid])])
		}
		way.Vals = newVals
	}
}

func updateRelationsStringIndexes(rels []*pbfproto.Relation, oldStringtable [][]byte, indexes map[string]int) {
	for _, rel := range rels {
		if rel.Info != nil && rel.Info.UserSid != nil {
			newIndex := uint32(indexes[string(oldStringtable[*rel.Info.UserSid])])
			rel.Info.UserSid = &newIndex
		}
		newKeys := make([]uint32, len(rel.Keys))
		for i, sid := range rel.Keys {
			newKeys[i] = uint32(indexes[string(oldStringtable[sid])])
		}
		rel.Keys = newKeys
		newVals := make([]uint32, len(rel.Vals))
		for i, sid := range rel.Vals {
			newVals[i] = uint32(indexes[string(oldStringtable[sid])])
		}
		rel.Vals = newVals
	}
}
