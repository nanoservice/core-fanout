// Code generated by protoc-gen-go.
// source: userneed.proto
// DO NOT EDIT!

/*
Package userneed is a generated protocol buffer package.

It is generated from these files:
	userneed.proto

It has these top-level messages:
	UserNeed
*/
package userneed

import proto "github.com/golang/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

type UserNeed struct {
	NeedId int64  `protobuf:"varint,1,opt,name=need_id" json:"need_id,omitempty"`
	UserId int64  `protobuf:"varint,2,opt,name=user_id" json:"user_id,omitempty"`
	Sender string `protobuf:"bytes,3,opt,name=sender" json:"sender,omitempty"`
}

func (m *UserNeed) Reset()         { *m = UserNeed{} }
func (m *UserNeed) String() string { return proto.CompactTextString(m) }
func (*UserNeed) ProtoMessage()    {}