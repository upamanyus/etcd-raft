// Copyright 2015 The etcd Authors
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

package raft

import (
	"bytes"
	"fmt"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

func (st StateType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", st.String())), nil
}

var isLocalMsg = [...]bool{
	raftpb.MsgHup:               true,
	raftpb.MsgBeat:              true,
	raftpb.MsgUnreachable:       true,
	raftpb.MsgSnapStatus:        true,
	raftpb.MsgCheckQuorum:       true,
	raftpb.MsgStorageAppend:     true,
	raftpb.MsgStorageAppendResp: true,
	raftpb.MsgStorageApply:      true,
	raftpb.MsgStorageApplyResp:  true,
}

var isResponseMsg = [...]bool{
	raftpb.MsgAppResp:           true,
	raftpb.MsgVoteResp:          true,
	raftpb.MsgHeartbeatResp:     true,
	raftpb.MsgUnreachable:       true,
	raftpb.MsgReadIndexResp:     true,
	raftpb.MsgPreVoteResp:       true,
	raftpb.MsgStorageAppendResp: true,
	raftpb.MsgStorageApplyResp:  true,
}

func isMsgInArray(msgt raftpb.MessageType, arr []bool) bool {
	i := int(msgt)
	return i < len(arr) && arr[i]
}

func IsLocalMsg(msgt raftpb.MessageType) bool {
	return isMsgInArray(msgt, isLocalMsg[:])
}

func IsResponseMsg(msgt raftpb.MessageType) bool {
	return isMsgInArray(msgt, isResponseMsg[:])
}

func IsLocalMsgTarget(id uint64) bool {
	return id == LocalAppendThread || id == LocalApplyThread
}

// voteResponseType maps vote and prevote message types to their corresponding responses.
func voteRespMsgType(msgt raftpb.MessageType) raftpb.MessageType {
	switch msgt {
	case raftpb.MsgVote:
		return raftpb.MsgVoteResp
	case raftpb.MsgPreVote:
		return raftpb.MsgPreVoteResp
	default:
		panic(fmt.Sprintf("not a vote message: %s", msgt))
	}
}

func DescribeHardState(hs raftpb.HardState) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "Term:%d", hs.Term)
	if hs.Vote != 0 {
		fmt.Fprintf(&buf, " Vote:%d", hs.Vote)
	}
	fmt.Fprintf(&buf, " Commit:%d", hs.Commit)
	return buf.String()
}

func DescribeSoftState(ss SoftState) string {
	return fmt.Sprintf("Lead:%d State:%s", ss.Lead, ss.RaftState)
}

func DescribeConfState(state raftpb.ConfState) string {
	return fmt.Sprintf(
		"Voters:%v VotersOutgoing:%v Learners:%v LearnersNext:%v AutoLeave:%v",
		state.Voters, state.VotersOutgoing, state.Learners, state.LearnersNext, state.AutoLeave,
	)
}

func DescribeSnapshot(snap raftpb.Snapshot) string {
	m := snap.Metadata
	return fmt.Sprintf("Index:%d Term:%d ConfState:%s", m.Index, m.Term, DescribeConfState(m.ConfState))
}

func DescribeReady(rd Ready, f EntryFormatter) string {
	var buf strings.Builder
	if rd.SoftState != nil {
		fmt.Fprint(&buf, DescribeSoftState(*rd.SoftState))
		buf.WriteByte('\n')
	}
	if !IsEmptyHardState(rd.HardState) {
		fmt.Fprintf(&buf, "HardState %s", DescribeHardState(rd.HardState))
		buf.WriteByte('\n')
	}
	if len(rd.ReadStates) > 0 {
		fmt.Fprintf(&buf, "ReadStates %v\n", rd.ReadStates)
	}
	if len(rd.Entries) > 0 {
		buf.WriteString("Entries:\n")
		fmt.Fprint(&buf, DescribeEntries(rd.Entries, f))
	}
	if !IsEmptySnap(rd.Snapshot) {
		fmt.Fprintf(&buf, "Snapshot %s\n", DescribeSnapshot(rd.Snapshot))
	}
	if len(rd.CommittedEntries) > 0 {
		buf.WriteString("CommittedEntries:\n")
		fmt.Fprint(&buf, DescribeEntries(rd.CommittedEntries, f))
	}
	if len(rd.Messages) > 0 {
		buf.WriteString("Messages:\n")
		for _, msg := range rd.Messages {
			fmt.Fprint(&buf, DescribeMessage(msg, f))
			buf.WriteByte('\n')
		}
	}
	if buf.Len() > 0 {
		return fmt.Sprintf("Ready MustSync=%t:\n%s", rd.MustSync, buf.String())
	}
	return "<empty Ready>"
}

// EntryFormatter can be implemented by the application to provide human-readable formatting
// of entry data. Nil is a valid EntryFormatter and will use a default format.
type EntryFormatter func([]byte) string

// DescribeMessage returns a concise human-readable description of a
// Message for debugging.
func DescribeMessage(m raftpb.Message, f EntryFormatter) string {
	return describeMessageWithIndent("", m, f)
}

func describeMessageWithIndent(indent string, m raftpb.Message, f EntryFormatter) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s->%s %v Term:%d Log:%d/%d", indent,
		describeTarget(m.From), describeTarget(m.To), m.Type, m.Term, m.LogTerm, m.Index)
	if m.Reject {
		fmt.Fprintf(&buf, " Rejected (Hint: %d)", m.RejectHint)
	}
	if m.Commit != 0 {
		fmt.Fprintf(&buf, " Commit:%d", m.Commit)
	}
	if m.Vote != 0 {
		fmt.Fprintf(&buf, " Vote:%d", m.Vote)
	}
	if ln := len(m.Entries); ln == 1 {
		fmt.Fprintf(&buf, " Entries:[%s]", DescribeEntry(m.Entries[0], f))
	} else if ln > 1 {
		fmt.Fprint(&buf, " Entries:[")
		for _, e := range m.Entries {
			fmt.Fprintf(&buf, "\n%s  ", indent)
			buf.WriteString(DescribeEntry(e, f))
		}
		fmt.Fprintf(&buf, "\n%s]", indent)
	}
	if s := m.Snapshot; s != nil && !IsEmptySnap(*s) {
		fmt.Fprintf(&buf, "\n%s  Snapshot: %s", indent, DescribeSnapshot(*s))
	}
	if len(m.Responses) > 0 {
		fmt.Fprintf(&buf, " Responses:[")
		for _, m := range m.Responses {
			buf.WriteString("\n")
			buf.WriteString(describeMessageWithIndent(indent+"  ", m, f))
		}
		fmt.Fprintf(&buf, "\n%s]", indent)
	}
	return buf.String()
}

func describeTarget(id uint64) string {
	switch id {
	case None:
		return "None"
	case LocalAppendThread:
		return "AppendThread"
	case LocalApplyThread:
		return "ApplyThread"
	default:
		return fmt.Sprintf("%x", id)
	}
}

// DescribeEntry returns a concise human-readable description of an
// Entry for debugging.
func DescribeEntry(e raftpb.Entry, f EntryFormatter) string {
	if f == nil {
		f = func(data []byte) string { return fmt.Sprintf("%q", data) }
	}

	formatConfChange := func(cc raftpb.ConfChangeI) string {
		// TODO(tbg): give the EntryFormatter a type argument so that it gets
		// a chance to expose the Context.
		return raftpb.ConfChangesToString(cc.AsV2().Changes)
	}

	var formatted string
	switch e.Type {
	case raftpb.EntryNormal:
		formatted = f(e.Data)
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(e.Data); err != nil {
			formatted = err.Error()
		} else {
			formatted = formatConfChange(cc)
		}
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(e.Data); err != nil {
			formatted = err.Error()
		} else {
			formatted = formatConfChange(cc)
		}
	}
	if formatted != "" {
		formatted = " " + formatted
	}
	return fmt.Sprintf("%d/%d %s%s", e.Term, e.Index, e.Type, formatted)
}

// DescribeEntries calls DescribeEntry for each Entry, adding a newline to
// each.
func DescribeEntries(ents []raftpb.Entry, f EntryFormatter) string {
	var buf bytes.Buffer
	for _, e := range ents {
		_, _ = buf.WriteString(DescribeEntry(e, f) + "\n")
	}
	return buf.String()
}

// entryEncodingSize represents the protocol buffer encoding size of one or more
// entries.
type entryEncodingSize uint64

func entsSize(ents []raftpb.Entry) entryEncodingSize {
	var size entryEncodingSize
	for _, ent := range ents {
		size += entryEncodingSize(ent.Size())
	}
	return size
}

// limitSize returns the longest prefix of the given entries slice, such that
// its total byte size does not exceed maxSize. Always returns a non-empty slice
// if the input is non-empty, so, as an exception, if the size of the first
// entry exceeds maxSize, a non-empty slice with just this entry is returned.
func limitSize(ents []raftpb.Entry, maxSize entryEncodingSize) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	for limit := 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if entryEncodingSize(size) > maxSize {
			return ents[:limit]
		}
	}
	return ents
}

// entryPayloadSize represents the size of one or more entries' payloads.
// Notably, it does not depend on its Index or Term. Entries with empty
// payloads, like those proposed after a leadership change, are considered
// to be zero size.
type entryPayloadSize uint64

// payloadSize is the size of the payload of the provided entry.
func payloadSize(e raftpb.Entry) entryPayloadSize {
	return entryPayloadSize(len(e.Data))
}

// payloadsSize is the size of the payloads of the provided entries.
func payloadsSize(ents []raftpb.Entry) entryPayloadSize {
	var s entryPayloadSize
	for _, e := range ents {
		s += payloadSize(e)
	}
	return s
}

func assertConfStatesEquivalent(l Logger, cs1, cs2 raftpb.ConfState) {
	err := cs1.Equivalent(cs2)
	if err == nil {
		return
	}
	l.Panic(err)
}

// extend appends vals to the given dst slice. It differs from the standard
// slice append only in the way it allocates memory. If cap(dst) is not enough
// for appending the values, precisely size len(dst)+len(vals) is allocated.
//
// Use this instead of standard append in situations when this is the last
// append to dst, so there is no sense in allocating more than needed.
func extend(dst, vals []raftpb.Entry) []raftpb.Entry {
	need := len(dst) + len(vals)
	if need <= cap(dst) {
		return append(dst, vals...) // does not allocate
	}
	buf := make([]raftpb.Entry, need, need) // allocates precisely what's needed
	copy(buf, dst)
	copy(buf[len(dst):], vals)
	return buf
}
