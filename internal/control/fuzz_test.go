package control

import (
	"crypto/ed25519"
	"testing"
	"time"

	"remount.dev/remount/internal/proto"
)

func FuzzVerifyGrant(f *testing.F) {
	f.Add(make([]byte, ed25519.PublicKeySize), make([]byte, ed25519.SignatureSize), "ws_seed", int64(1))
	f.Add([]byte{1}, []byte{2}, "", int64(-1))
	f.Fuzz(func(t *testing.T, publicKey, signature []byte, workspace string, expires int64) {
		if len(publicKey) > 1024 || len(signature) > 4096 || len(workspace) > 4096 {
			t.Skip()
		}
		grant := &proto.Grant{
			Claims:    proto.GrantClaims{WS: workspace, ExpiresAt: expires},
			Signature: signature,
		}
		_ = VerifyGrant(ed25519.PublicKey(publicKey), grant, time.UnixMilli(0))
	})
}

func FuzzWorkspaceLifecycle(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 1, 2, 0, 1, 3, 0, 1})
	f.Add([]byte{1, 1, 255, 0, 0, 0, 1, 1, 0})
	f.Fuzz(func(t *testing.T, commands []byte) {
		if len(commands) > 12_000 {
			t.Skip()
		}
		workspace := proto.Workspace{ID: "ws_fuzz", State: proto.WSPending}
		nodes := [...]string{"n_one", "n_two"}
		for offset := 0; offset+2 < len(commands); offset += 3 {
			command := commands[offset] % 6
			node := nodes[commands[offset+1]%byte(len(nodes))]
			generation := workspace.Generation
			if commands[offset+2]&1 != 0 {
				generation ^= uint64(commands[offset+2])
			}
			request := lifecycleTransition{expectGeneration: true, generation: generation}
			switch command {
			case 0: // claim or duplicate claim
				request.operation, request.actor, request.to = transitionClaim, actorNode, proto.WSClaiming
				if workspace.State != proto.WSPending {
					request.expectNode, request.node = true, node
				}
			case 1: // delayed or current ready
				request.operation, request.actor, request.to = transitionReady, actorNode, proto.WSClaimed
				request.expectNode, request.node = true, node
			case 2: // node reports release
				request.operation, request.actor, request.to = transitionNodeReleased, actorNode, proto.WSPending
				request.expectNode, request.node = true, node
			case 3: // lease expires
				request.operation, request.actor, request.to = transitionLeaseExpired, actorControl, proto.WSPending
				request.expectNode, request.node = true, node
			case 4: // operator quarantine
				request.operation, request.actor, request.to = transitionFleetComplete, actorControl, proto.WSFailed
			case 5: // terminal destroy
				request.operation, request.actor, request.to = transitionFleetComplete, actorControl, proto.WSDestroyed
			}
			before := workspace
			next, err := transitionWorkspace(&workspace, request)
			if err != nil {
				if workspace.State != before.State || workspace.Generation != before.Generation || workspace.Node != before.Node {
					t.Fatal("rejected transition mutated input")
				}
				continue
			}
			if command == 0 && before.State == proto.WSPending {
				next.Generation++
				next.Node = node
			}
			if command == 2 || command == 3 || command == 5 {
				next.Node = ""
			}
			workspace = next
			if workspace.State == proto.WSClaimed && workspace.Node == "" {
				t.Fatal("serviceable workspace has no authoritative node")
			}
			if before.State == proto.WSDestroyed {
				t.Fatal("destroyed state was not absorbing")
			}
		}
	})
}
