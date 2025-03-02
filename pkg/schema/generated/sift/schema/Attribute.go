// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package schema

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Attribute struct {
	_tab flatbuffers.Table
}

func GetRootAsAttribute(buf []byte, offset flatbuffers.UOffsetT) *Attribute {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Attribute{}
	x.Init(buf, n+offset)
	return x
}

func FinishAttributeBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsAttribute(buf []byte, offset flatbuffers.UOffsetT) *Attribute {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Attribute{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedAttributeBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Attribute) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Attribute) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Attribute) Key() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Attribute) Value() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func AttributeStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func AttributeAddKey(builder *flatbuffers.Builder, key flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(key), 0)
}
func AttributeAddValue(builder *flatbuffers.Builder, value flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(value), 0)
}
func AttributeEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
