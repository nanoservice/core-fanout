// Code generated by protoc-gen-go.
// source: heatbeat.proto
// DO NOT EDIT!

/*
Package heatbeat is a generated protocol buffer package.

It is generated from these files:
	heatbeat.proto

It has these top-level messages:
	Heartbeat
*/
package heatbeat

import proto "github.com/golang/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

type Heartbeat struct {
	Id    string `protobuf:"bytes,1,opt,name=id" json:"id,omitempty"`
	Topic string `protobuf:"bytes,2,opt,name=topic" json:"topic,omitempty"`
}

func (m *Heartbeat) Reset()         { *m = Heartbeat{} }
func (m *Heartbeat) String() string { return proto.CompactTextString(m) }
func (*Heartbeat) ProtoMessage()    {}