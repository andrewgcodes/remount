package main

import (
	"context"
	"strings"
	"testing"

	"remount.dev/remount/internal/proto"
)

func TestComputerSubcommandsValidateBeforeDialing(t *testing.T) {
	if err := cmdComputer(context.Background(), nil); err == nil {
		t.Fatal("computer without subcommand accepted")
	}
	if err := cmdComputer(context.Background(), []string{"wat", "ws_1"}); err == nil ||
		!strings.Contains(err.Error(), "unknown computer subcommand") {
		t.Fatalf("error=%v", err)
	}
	if err := cmdComputer(context.Background(), []string{"get", "ws_1"}); err == nil {
		t.Fatal("computer get without a computer id accepted")
	}
	// A PNG written to a terminal is line noise, so the flag is required and
	// the check happens before anything dials.
	err := cmdComputer(context.Background(), []string{"screenshot", "ws_1", "cmp_1"})
	if err == nil || !strings.Contains(err.Error(), "--out") {
		t.Fatalf("error=%v", err)
	}
	if err := cmdComputer(context.Background(), []string{"click", "ws_1", "cmp_1", "10"}); err == nil {
		t.Fatal("click with one coordinate accepted")
	}
	err = cmdComputer(context.Background(), []string{"click", "ws_1", "cmp_1", "ten", "20"})
	if err == nil || !strings.Contains(err.Error(), "integer coordinate") {
		t.Fatalf("error=%v", err)
	}
	err = cmdComputer(context.Background(), []string{"create", "ws_1", "--viewport", "1280"})
	if err == nil || !strings.Contains(err.Error(), "WxH") {
		t.Fatalf("error=%v", err)
	}
}

// Go's flag package stops at the first positional, so a viewport typed after
// the workspace id would be silently dropped without parse's permutation.
func TestComputerCreateAcceptsFlagsAfterPositionals(t *testing.T) {
	err := cmdComputer(context.Background(), []string{"create", "ws_1", "--viewport", "800xtall"})
	if err == nil || !strings.Contains(err.Error(), "height must be a positive integer") {
		t.Fatalf("the flag after the positional was not parsed: %v", err)
	}
}

func TestParseViewportRejectsAnythingButWxH(t *testing.T) {
	if w, h, err := parseViewport("1024X768"); err != nil || w != 1024 || h != 768 {
		t.Fatalf("parseViewport(1024X768) = %d,%d,%v", w, h, err)
	}
	for _, bad := range []string{"", "1280", "0x720", "1280x0", "-1x720", "axb", "1280x720x1"} {
		if _, _, err := parseViewport(bad); err == nil {
			t.Fatalf("parseViewport(%q) was accepted", bad)
		}
	}
}

func TestModifierFlagsMapToTheCDPBits(t *testing.T) {
	m := modifierFlags{alt: true, shift: true}
	want := proto.ComputerModifierAlt | proto.ComputerModifierShift
	if got := m.bits(); got != want {
		t.Fatalf("bits = %d, want %d", got, want)
	}
	if (&modifierFlags{}).bits() != 0 {
		t.Fatal("no modifiers must be zero")
	}
}
