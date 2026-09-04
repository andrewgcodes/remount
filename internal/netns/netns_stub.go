//go:build !linux

package netns

import (
	"context"
	"errors"
	"net/netip"
)

type systemKernel struct{}

func newSystemKernel() Kernel { return systemKernel{} }

func (systemKernel) unavailable() error                             { return errors.New("network namespaces require Linux") }
func (k systemKernel) Probe(context.Context) error                  { return k.unavailable() }
func (k systemKernel) Validate(context.Context, string, Link) error { return k.unavailable() }
func (k systemKernel) CreateNamespace(context.Context, string) (string, error) {
	return "", k.unavailable()
}
func (k systemKernel) CreateVeth(context.Context, string, string, string) error {
	return k.unavailable()
}
func (k systemKernel) Configure(context.Context, string, Link) error { return k.unavailable() }
func (k systemKernel) InstallDenyAll(context.Context, string, Link) error {
	return k.unavailable()
}
func (k systemKernel) PermitBroker(context.Context, string, Link, netip.AddrPort) error {
	return k.unavailable()
}
func (k systemKernel) BringUp(context.Context, string, string) error { return k.unavailable() }
func (k systemKernel) DeleteVeth(context.Context, string) error      { return k.unavailable() }
func (systemKernel) CloseNamespace(string) error                     { return nil }
func (k systemKernel) CreateTap(context.Context, string, string, int, int, netip.Prefix, Link) error {
	return k.unavailable()
}
func (k systemKernel) RouteTap(context.Context, netip.Prefix, Link) error {
	return k.unavailable()
}
func (k systemKernel) DeleteTap(context.Context, string, string) error { return k.unavailable() }
