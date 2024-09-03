package raft

import (
	"github.com/stretchr/testify/assert"
	"go.etcd.io/raft/v3/raftpb"
	"math/rand"
	"testing"
)

func newTestConfig(id uint64, election, heartbeat int, storage Storage) *Config {
	return &Config{
		ID:              id,
		ElectionTick:    election,
		HeartbeatTick:   heartbeat,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 256,
	}
}

func entsWithConfig(configFunc func(*Config), terms ...uint64) *raft {
	storage := NewMemoryStorage()
	for i, term := range terms {
		storage.Append([]raftpb.Entry{{Index: uint64(i + 1), Term: term}})
	}
	cfg := newTestConfig(1, 5, 1, storage)
	if configFunc != nil {
		configFunc(cfg)
	}
	sm := newRaft(cfg)
	sm.reset(terms[len(terms)-1])
	return sm
}

type blackHole struct{}

func (blackHole) Step(raftpb.Message) error      { return nil }
func (blackHole) readMessages() []raftpb.Message { return nil }
func (blackHole) advanceMessagesAfterAppend()    {}

var nopStepper = &blackHole{}

func preVoteConfig(c *Config) {
	c.PreVote = true
}

type stateMachine interface {
	Step(m raftpb.Message) error
	readMessages() []raftpb.Message
	advanceMessagesAfterAppend()
}

type connem struct {
	from, to uint64
}

type network struct {
	t *testing.T // optional

	peers   map[uint64]stateMachine
	storage map[uint64]*MemoryStorage
	dropm64   map[connem]uint64
	ignorem map[raftpb.MessageType]bool

	// msgHook is called for each message sent. It may inspect the
	// message and return true to send it or false to drop it.
	msgHook func(raftpb.Message) bool
}

func idsBySize(size int) []uint64 {
	ids := make([]uint64, size)
	for i := 0; i < size; i++ {
		ids[i] = 1 + uint64(i)
	}
	return ids
}

func withPeers(peers ...uint64) testMemoryStorageOptions {
	return func(ms *MemoryStorage) {
		ms.snapshot.Metadata.ConfState.Voters = peers
	}
}

type testMemoryStorageOptions func(*MemoryStorage)

func newTestMemoryStorage(opts ...testMemoryStorageOptions) *MemoryStorage {
	ms := NewMemoryStorage()
	for _, o := range opts {
		o(ms)
	}
	return ms
}

func (r *raft) readMessages() []raftpb.Message {
	r.advanceMessagesAfterAppend()
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

func (r *raft) takeMessagesAfterAppend() []raftpb.Message {
	msgs := r.msgsAfterAppend
	r.msgsAfterAppend = nil
	return msgs
}

func (r *raft) stepOrSend(msgs []raftpb.Message) error {
	for _, m := range msgs {
		if m.To == r.id {
			if err := r.Step(m); err != nil {
				return err
			}
		} else {
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}

func (r *raft) advanceMessagesAfterAppend() {
	for {
		msgs := r.takeMessagesAfterAppend()
		if len(msgs) == 0 {
			break
		}
		r.stepOrSend(msgs)
	}
}

func newNetworkWithConfigInit(configFunc func(*Config), peers ...stateMachine) *network {
	size := len(peers)
	peerAddrs := idsBySize(size)

	npeers := make(map[uint64]stateMachine, size)
	nstorage := make(map[uint64]*MemoryStorage, size)

	for j, p := range peers {
		id := peerAddrs[j]
		if p == nil {
			nstorage[id] = newTestMemoryStorage(withPeers(peerAddrs...))
			cfg := newTestConfig(id, 10, 1, nstorage[id])
			if configFunc != nil {
				configFunc(cfg)
			}
			sm := newRaft(cfg)
			npeers[id] = sm
		} else {
			npeers[id] = p
		}
	}
	return &network{
		peers:   npeers,
		storage: nstorage,
		dropm64:   make(map[connem]uint64),
		ignorem: make(map[raftpb.MessageType]bool),
	}
}

func (nw *network) filter(msgs []raftpb.Message) []raftpb.Message {
	var mm []raftpb.Message
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case raftpb.MsgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected msgHup")
		default:
			perc64 := nw.dropm64[connem{m.From, m.To}]
			if n := rand.Uint64(); n < perc64 {
				continue
			}
		}
		if nw.msgHook != nil {
			if !nw.msgHook(m) {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

func (nw *network) send(msgs ...raftpb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		if nw.t != nil {
			nw.t.Log(DescribeMessage(m, nil))
		}
		_ = p.Step(m)
		p.advanceMessagesAfterAppend()
		msgs = append(msgs[1:], nw.filter(p.readMessages())...)
	}
}

type testLeaderElectionStruct struct {
	*network
	state   StateType
	expTerm uint64
}
func testLeaderElection2(t *testing.T, preVote bool) {
	var cfg func(*Config)
	candState := StateCandidate
	candTerm := uint64(1)
	if preVote {
		cfg = preVoteConfig
		// In pre-vote mode, an election that fails to complete
		// leaves the node in pre-candidate state without advancing
		// the term.
		candState = StatePreCandidate
		candTerm = 0
	}
	tests := []testLeaderElectionStruct{
		{newNetworkWithConfigInit(cfg, nil, nil, nil), StateLeader, 1},
		{newNetworkWithConfigInit(cfg, nil, nil, nopStepper), StateLeader, 1},
		{newNetworkWithConfigInit(cfg, nil, nopStepper, nopStepper), candState, candTerm},
		{newNetworkWithConfigInit(cfg, nil, nopStepper, nopStepper, nil), candState, candTerm},
		{newNetworkWithConfigInit(cfg, nil, nopStepper, nopStepper, nil, nil), StateLeader, 1},

		// three logs further along than 0, but in the same term so rejections
		// are returned instead of the votes being ignored.
		{newNetworkWithConfigInit(cfg,
			nil, entsWithConfig(cfg, 1), entsWithConfig(cfg, 1), entsWithConfig(cfg, 1, 1), nil),
			StateFollower, 1},
	}

	for i, tt := range tests {
		tt.send(raftpb.Message{From: 1, To: 1, Type: raftpb.MsgHup})
		sm := tt.network.peers[1].(*raft)
		assert.Equal(t, tt.state, sm.state, "#%d", i)
		assert.Equal(t, tt.expTerm, sm.Term, "#%d", i)
	}
}
