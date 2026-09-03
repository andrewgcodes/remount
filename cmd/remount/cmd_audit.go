package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/compliance"
	"remount.dev/remount/internal/proto"
)

// cmdAudit implements the compliance-export command group.
func cmdAudit(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("audit: export|verify|key")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("audit "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "export":
		tenant := fs.String("tenant", "", "tenant to export; defaults to the caller's own tenant")
		rangeSpec := fs.String("range", "", "sequence range FROM..TO, inclusive of both endpoints")
		out := fs.String("out", "", "write the bundle to this file instead of stdout; never overwrites an existing file")
		parse(fs, rest)
		if err := arity(fs, 0, 0, "audit export --tenant T --range FROM..TO [--out FILE]"); err != nil {
			return err
		}
		from, to, err := parseAuditRange(*rangeSpec)
		if err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		var result proto.AuditExportRes
		if err := controlCall(ctx, cl, proto.OpAuditExport, proto.AuditExportReq{Tenant: *tenant, From: from, To: to}, &result); err != nil {
			return err
		}
		if err := writeAuditBundle(ctx, *out, result.Bundle); err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(result.Manifest)
			return nil
		}
		fmt.Fprintf(os.Stderr, "%d events, sequences %d..%d inclusive, sha256 %s, key %s\n",
			result.Manifest.EventCount, result.Manifest.RangeFrom, result.Manifest.RangeTo,
			result.Manifest.PayloadSHA256, result.Manifest.KeyID)
	case "verify":
		key := fs.String("key", "", "base64url ed25519 public key; without it the key is fetched from the server")
		tenant := fs.String("tenant", "", "require the bundle to name this tenant")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "audit verify BUNDLE [--key BASE64] [--tenant T]"); err != nil {
			return err
		}
		keys, err := auditVerificationKey(ctx, &commonFlags, *key)
		if err != nil {
			return err
		}
		bundle, err := os.Open(fs.Arg(0))
		if err != nil {
			return err
		}
		defer bundle.Close()
		manifest, err := compliance.Verify(ctx, bundle, keys, compliance.VerifyOptions{ExpectedTenant: *tenant})
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(manifest)
			return nil
		}
		fmt.Printf("verified %d events for tenant %s, sequences %d..%d inclusive, signed by %s\n",
			manifest.EventCount, manifest.Tenant, manifest.RangeFrom, manifest.RangeTo, manifest.KeyID)
	case "key":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "audit key"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		var result proto.AuditKeyRes
		if err := controlCall(ctx, cl, proto.OpAuditKey, proto.AuditKeyReq{}, &result); err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(result)
			return nil
		}
		fmt.Println(result.KeyID, result.Algorithm, result.PublicKey)
	default:
		return fmt.Errorf("unknown audit subcommand %q", sub)
	}
	return nil
}

// parseAuditRange reads FROM..TO.
//
// Both endpoints are INCLUSIVE: 10..20 is eleven sequences wide and contains
// both 10 and 20. Sequence numbering starts at 1, so FROM must be at
// least 1; an omitted endpoint is rejected rather than defaulting, because a
// range that silently widened would produce a bundle covering more than the
// operator asked to disclose. A three-dot form is refused outright so it can
// never be mistaken for a different, exclusive spelling of the same range.
func parseAuditRange(spec string) (uint64, uint64, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, 0, errors.New("--range FROM..TO is required, inclusive of both endpoints")
	}
	if strings.Contains(spec, "...") {
		return 0, 0, errors.New("--range uses two dots: FROM..TO includes both FROM and TO")
	}
	first, second, found := strings.Cut(spec, "..")
	if !found {
		return 0, 0, fmt.Errorf("--range %q is not FROM..TO", spec)
	}
	from, err := strconv.ParseUint(strings.TrimSpace(first), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("--range start %q is not a sequence number", first)
	}
	to, err := strconv.ParseUint(strings.TrimSpace(second), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("--range end %q is not a sequence number", second)
	}
	if from == 0 {
		return 0, 0, errors.New("--range starts at sequence 1; 0 is not a sequence")
	}
	if to < from {
		return 0, 0, fmt.Errorf("--range %d..%d ends before it starts", from, to)
	}
	return from, to, nil
}

// writeAuditBundle publishes the bundle verbatim. The bytes are what the
// manifest's hash covers, so they are never re-encoded, and the destination is
// created atomically at mode 0600 and never clobbered: an audit bundle that
// silently replaced an earlier one would destroy the evidence it was meant to
// preserve.
func writeAuditBundle(ctx context.Context, path string, bundle []byte) error {
	if path == "" {
		_, err := os.Stdout.Write(bundle)
		return err
	}
	sink := &compliance.AtomicFileSink{Path: path}
	return sink.Commit(ctx, func(w io.Writer) error {
		_, err := w.Write(bundle)
		return err
	})
}

// pinnedAuditKey answers with the one key the operator supplied, whatever key
// id the manifest names. Trusting the operator's key rather than the bundle's
// id is what makes offline verification meaningful; the signature check is
// unchanged, so a bundle signed by any other key still fails.
type pinnedAuditKey struct{ key ed25519.PublicKey }

func (p pinnedAuditKey) ResolvePublicKey(context.Context, string) (ed25519.PublicKey, error) {
	return p.key, nil
}

// auditVerificationKey resolves what a bundle must verify against. An explicit
// --key keeps verification offline, which is the point of a signed bundle;
// without one the CLI asks the control plane for the public half, which is
// convenient but proves less, because the same authority produced the bundle.
func auditVerificationKey(ctx context.Context, commonFlags *common, encoded string) (compliance.PublicKeyResolver, error) {
	if encoded != "" {
		raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, fmt.Errorf("--key is not base64url: %w", err)
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("--key is %d bytes, not an ed25519 public key", len(raw))
		}
		return pinnedAuditKey{key: ed25519.PublicKey(raw)}, nil
	}
	cl := commonFlags.client()
	defer cl.Close()
	var result proto.AuditKeyRes
	if err := controlCall(ctx, cl, proto.OpAuditKey, proto.AuditKeyReq{}, &result); err != nil {
		return nil, err
	}
	raw, err := base64.RawURLEncoding.DecodeString(result.PublicKey)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("the server returned a malformed audit verification key")
	}
	return compliance.NewKeyring(map[string]ed25519.PublicKey{result.KeyID: ed25519.PublicKey(raw)})
}

// controlCall issues one control-plane request over the client's live peer.
// Audit export is an administrative one-shot with no reconnect semantics of
// its own, so it uses the peer directly rather than growing the SDK surface
// with a method the CLI would be the only caller of.
func controlCall(ctx context.Context, cl *client.Client, op string, body, out any) error {
	peer, err := cl.Connect(ctx)
	if err != nil {
		return err
	}
	return peer.Call(ctx, proto.PeerControl, op, body, out)
}
