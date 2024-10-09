package pbfproto

import "github.com/planetscale/vtprotobuf/protohelpers"

var groupSizes []int

func (m *PrimitiveBlock) ClearGroupSizeCache() {
	groupSizes = nil
}

// MySize is a custom size caculation function based on the one from
// vtprotobuf. It is improved by using a cache for previously calculated
// group sizes.
//
// Before using it on a new PrimitiveBlock, ClearGroupSizeCache must be
// called.
//
// It may only be used for a single PrimitiveBlock at a time, because it
// uses a global cache.
func (m *PrimitiveBlock) MySize() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Stringtable != nil {
		l = m.Stringtable.SizeVT()
		n += 1 + l + protohelpers.SizeOfVarint(uint64(l))
	}
	if len(m.Primitivegroup) > 0 {
		for i, e := range m.Primitivegroup {
			var l int
			if i < len(groupSizes) {
				l = (groupSizes)[i]
			} else {
				l = e.SizeVT()
				groupSizes = append(groupSizes, l)
			}
			n += 1 + l + protohelpers.SizeOfVarint(uint64(l))
		}
	}
	if m.Granularity != nil {
		n += 2 + protohelpers.SizeOfVarint(uint64(*m.Granularity))
	}
	if m.DateGranularity != nil {
		n += 2 + protohelpers.SizeOfVarint(uint64(*m.DateGranularity))
	}
	if m.LatOffset != nil {
		n += 2 + protohelpers.SizeOfVarint(uint64(*m.LatOffset))
	}
	if m.LonOffset != nil {
		n += 2 + protohelpers.SizeOfVarint(uint64(*m.LonOffset))
	}
	n += len(m.unknownFields)
	return n
}
