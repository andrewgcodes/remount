//go:build linux

package netns

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	vethInfoPeer = 1
	nlaNested    = uint16(1 << 15)

	// namespaceDir holds the bind mounts that keep a workspace's network
	// namespace alive across the process that created it.
	namespaceDir = "/run/remount/netns"

	nfDrop   = 0
	nfAccept = 1

	// netdevTable and netdevChain hold the egress filter that actually
	// contains a userspace network stack; see installNetdevDenyTable.
	netdevTable = "remount_dev"
	netdevChain = "egress_dev"
)

type systemKernel struct {
	seq atomic.Uint32
}

func newSystemKernel() Kernel {
	return &systemKernel{}
}

func (k *systemKernel) Probe(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return fmt.Errorf("read effective capabilities: %w", err)
	}
	var effective uint64
	for _, line := range strings.Split(string(status), "\n") {
		if value, ok := strings.CutPrefix(line, "CapEff:\t"); ok {
			effective, err = strconv.ParseUint(strings.TrimSpace(value), 16, 64)
			break
		}
	}
	if err != nil || effective&(uint64(1)<<unix.CAP_NET_ADMIN) == 0 {
		return errors.New("CAP_NET_ADMIN is unavailable")
	}
	if effective&(uint64(1)<<unix.CAP_SYS_ADMIN) == 0 {
		return errors.New("CAP_SYS_ADMIN is unavailable for network namespace creation")
	}
	for _, protocol := range []int{unix.NETLINK_ROUTE, unix.NETLINK_NETFILTER} {
		fd, socketErr := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, protocol)
		if socketErr != nil {
			return fmt.Errorf("open netlink protocol %d: %w", protocol, socketErr)
		}
		if closeErr := unix.Close(fd); closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func (k *systemKernel) Validate(ctx context.Context, namespace string, link Link) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if filepath.Dir(filepath.Clean(namespace)) != namespaceDir {
		return fmt.Errorf("namespace %q is outside %s", namespace, namespaceDir)
	}
	if _, err := os.Stat(namespace); err != nil {
		return err
	}
	iface, err := net.InterfaceByName(link.HostName)
	if err != nil {
		return err
	}
	if iface.Flags&net.FlagUp == 0 {
		return fmt.Errorf("host veth %s is down", link.HostName)
	}
	return k.withNamespace(namespace, func() error {
		_, err := net.InterfaceByName(link.GuestName)
		return err
	})
}

func (k *systemKernel) CreateNamespace(ctx context.Context, name string) (path string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := os.Open("/proc/thread-self/ns/net")
	if err != nil {
		return "", err
	}
	defer original.Close()
	dir := namespaceDir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	path = dir + "/" + name
	target, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDONLY, 0o600)
	if err != nil {
		return "", err
	}
	_ = target.Close()
	removeTarget := true
	defer func() {
		if removeTarget {
			_ = os.Remove(path)
		}
	}()
	if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
		return "", err
	}
	createErr := unix.Mount("/proc/thread-self/ns/net", path, "none", unix.MS_BIND, "")
	restoreErr := unix.Setns(int(original.Fd()), unix.CLONE_NEWNET)
	if restoreErr != nil {
		// Returning this thread to Go's scheduler in the wrong namespace would
		// silently alter unrelated work, so keep it pinned and fail loudly.
		runtime.LockOSThread()
		return "", fmt.Errorf("restore host network namespace: %w", restoreErr)
	}
	if createErr != nil {
		return "", createErr
	}
	removeTarget = false
	return path, nil
}

func (k *systemKernel) CreateVeth(ctx context.Context, namespace, hostName, guestName string) error {
	attrs := append(nlaString(unix.IFLA_IFNAME, hostName),
		nlaNestedAttr(unix.IFLA_LINKINFO,
			append(nlaString(unix.IFLA_INFO_KIND, "veth"),
				nlaNestedAttr(unix.IFLA_INFO_DATA,
					nlaNestedAttr(vethInfoPeer,
						append(marshal(unix.IfInfomsg{Family: unix.AF_UNSPEC}), nlaString(unix.IFLA_IFNAME, guestName)...)))...))...)
	msg := append(marshal(unix.IfInfomsg{Family: unix.AF_UNSPEC}), attrs...)
	if err := k.route(ctx, unix.RTM_NEWLINK, unix.NLM_F_CREATE|unix.NLM_F_EXCL, msg); err != nil {
		return err
	}
	guest, err := net.InterfaceByName(guestName)
	if err != nil {
		k.deleteVethBestEffort(hostName)
		return err
	}
	file, err := os.Open(namespace)
	if err != nil {
		k.deleteVethBestEffort(hostName)
		return err
	}
	defer file.Close()
	set := unix.IfInfomsg{Family: unix.AF_UNSPEC, Index: int32(guest.Index)}
	msg = append(marshal(set), nlaU32Native(unix.IFLA_NET_NS_FD, uint32(file.Fd()))...)
	if err := k.route(ctx, unix.RTM_SETLINK, 0, msg); err != nil {
		k.deleteVethBestEffort(hostName)
		return err
	}
	return nil
}

func (k *systemKernel) deleteVethBestEffort(hostName string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = k.DeleteVeth(cleanupCtx, hostName)
}

func (k *systemKernel) Configure(ctx context.Context, namespace string, link Link) error {
	host, err := net.InterfaceByName(link.HostName)
	if err != nil {
		return err
	}
	if err := k.addAddress(ctx, host.Index, link.Host); err != nil {
		return err
	}
	return k.withNamespace(namespace, func() error {
		guest, err := net.InterfaceByName(link.GuestName)
		if err != nil {
			return err
		}
		if err := k.addAddress(ctx, guest.Index, link.Guest); err != nil {
			return err
		}
		// The host peer remains down, so raising the guest cannot create an
		// egress interval. Linux nevertheless requires this connected route to
		// be usable before accepting a default route through the host address.
		if err := k.setLinkUp(ctx, guest.Index); err != nil {
			return err
		}
		return k.addDefaultRoute(ctx, guest.Index, link.Host.Addr())
	})
}

func (k *systemKernel) InstallDenyAll(ctx context.Context, namespace string) error {
	return k.withNamespace(namespace, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, path := range []string{
			"/proc/sys/net/ipv6/conf/all/disable_ipv6",
			"/proc/sys/net/ipv6/conf/default/disable_ipv6",
		} {
			if err := os.WriteFile(path, []byte("1\n"), 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("disable IPv6: %w", err)
			}
		}
		if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o600); err != nil {
			return fmt.Errorf("enable namespace forwarding: %w", err)
		}
		if err := k.installDenyTable(ctx); err != nil {
			return err
		}
		return k.installNetdevDenyTable(ctx)
	})
}

func (k *systemKernel) PermitBroker(ctx context.Context, namespace string, broker netip.AddrPort) error {
	return k.withNamespace(namespace, func() error {
		if err := k.installPermitRule(ctx, broker); err != nil {
			return err
		}
		return k.installForwardPermitRules(ctx, broker)
	})
}

func (k *systemKernel) CreateTap(ctx context.Context, namespace, name string, uid, gid int, gateway netip.Prefix) error {
	return k.withNamespace(namespace, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		request, err := unix.NewIfreq(name)
		if err != nil {
			return err
		}
		request.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI)
		if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, request); err != nil {
			return err
		}
		if err := unix.IoctlSetInt(fd, unix.TUNSETOWNER, uid); err != nil {
			return err
		}
		if err := unix.IoctlSetInt(fd, unix.TUNSETGROUP, gid); err != nil {
			return err
		}
		if err := unix.IoctlSetInt(fd, unix.TUNSETPERSIST, 1); err != nil {
			return err
		}
		iface, err := net.InterfaceByName(name)
		if err != nil {
			return err
		}
		if err := k.addAddress(ctx, iface.Index, gateway); err != nil {
			return err
		}
		return k.setLinkUp(ctx, iface.Index)
	})
}

func (k *systemKernel) DeleteTap(ctx context.Context, namespace, name string) error {
	return k.withNamespace(namespace, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		request, err := unix.NewIfreq(name)
		if err != nil {
			return err
		}
		request.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI)
		if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, request); err != nil {
			if errors.Is(err, unix.ENODEV) {
				return nil
			}
			return err
		}
		return unix.IoctlSetInt(fd, unix.TUNSETPERSIST, 0)
	})
}

func (k *systemKernel) BringUp(ctx context.Context, namespace, hostName string) error {
	host, err := net.InterfaceByName(hostName)
	if err != nil {
		return err
	}
	if err := k.setLinkUp(ctx, host.Index); err != nil {
		return err
	}
	return k.withNamespace(namespace, func() error {
		for _, name := range []string{"lo", guestNameForHost(hostName)} {
			iface, err := net.InterfaceByName(name)
			if err != nil {
				return err
			}
			if err := k.setLinkUp(ctx, iface.Index); err != nil {
				return err
			}
		}
		return nil
	})
}

func guestNameForHost(host string) string {
	if len(host) >= 3 && host[:3] == "rmh" {
		return "rmg" + host[3:]
	}
	return host
}

func (k *systemKernel) DeleteVeth(ctx context.Context, hostName string) error {
	iface, err := net.InterfaceByName(hostName)
	if err != nil {
		if errors.Is(err, unix.ENODEV) || strings.Contains(err.Error(), "no such network interface") {
			return nil
		}
		return err
	}
	msg := marshal(unix.IfInfomsg{Family: unix.AF_UNSPEC, Index: int32(iface.Index)})
	if err := k.route(ctx, unix.RTM_DELLINK, 0, msg); err != nil && !errors.Is(err, unix.ENODEV) {
		return err
	}
	return nil
}

func (k *systemKernel) CloseNamespace(path string) error {
	if err := unix.Unmount(path, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (k *systemKernel) withNamespace(path string, fn func() error) (err error) {
	target, err := os.Open(path)
	if err != nil {
		return err
	}
	defer target.Close()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := os.Open("/proc/thread-self/ns/net")
	if err != nil {
		return err
	}
	defer original.Close()
	if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
		return err
	}
	defer func() {
		if restoreErr := unix.Setns(int(original.Fd()), unix.CLONE_NEWNET); restoreErr != nil {
			// See CreateNamespace: never release a contaminated OS thread.
			runtime.LockOSThread()
			err = errors.Join(err, fmt.Errorf("restore host network namespace: %w", restoreErr))
		}
	}()
	return fn()
}

func (k *systemKernel) addAddress(ctx context.Context, index int, prefix netip.Prefix) error {
	ip := prefix.Addr().AsSlice()
	msg := marshal(unix.IfAddrmsg{Family: unix.AF_INET, Prefixlen: uint8(prefix.Bits()), Index: uint32(index)})
	msg = append(msg, nlaBytes(unix.IFA_LOCAL, ip)...)
	msg = append(msg, nlaBytes(unix.IFA_ADDRESS, ip)...)
	return k.route(ctx, unix.RTM_NEWADDR, unix.NLM_F_CREATE|unix.NLM_F_EXCL, msg)
}

func (k *systemKernel) addDefaultRoute(ctx context.Context, index int, gateway netip.Addr) error {
	route := unix.RtMsg{Family: unix.AF_INET, Table: unix.RT_TABLE_MAIN, Protocol: unix.RTPROT_STATIC, Scope: unix.RT_SCOPE_UNIVERSE, Type: unix.RTN_UNICAST}
	msg := append(marshal(route), nlaU32Native(unix.RTA_OIF, uint32(index))...)
	msg = append(msg, nlaBytes(unix.RTA_GATEWAY, gateway.AsSlice())...)
	return k.route(ctx, unix.RTM_NEWROUTE, unix.NLM_F_CREATE|unix.NLM_F_EXCL, msg)
}

func (k *systemKernel) setLinkUp(ctx context.Context, index int) error {
	info := unix.IfInfomsg{Family: unix.AF_UNSPEC, Index: int32(index), Flags: unix.IFF_UP, Change: unix.IFF_UP}
	return k.route(ctx, unix.RTM_NEWLINK, 0, marshal(info))
}

func (k *systemKernel) route(ctx context.Context, typ uint16, flags uint16, body []byte) error {
	return k.netlink(ctx, unix.NETLINK_ROUTE, typ, flags, body)
}

func (k *systemKernel) netlink(ctx context.Context, protocol int, typ uint16, flags uint16, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, protocol)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1}); err != nil {
		return err
	}
	seq := k.seq.Add(1)
	header := unix.NlMsghdr{Len: uint32(unix.NLMSG_HDRLEN + len(body)), Type: typ, Flags: unix.NLM_F_REQUEST | unix.NLM_F_ACK | flags, Seq: seq}
	packet := append(marshal(header), body...)
	if err := unix.Sendto(fd, packet, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return err
	}
	buf := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				continue
			}
			return err
		}
		messages, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return err
		}
		for _, message := range messages {
			if message.Header.Seq != seq {
				continue
			}
			if message.Header.Type == unix.NLMSG_ERROR {
				if len(message.Data) < 4 {
					return errors.New("short netlink acknowledgement")
				}
				code := int32(nativeEndian.Uint32(message.Data[:4]))
				if code == 0 {
					return nil
				}
				return unix.Errno(-code)
			}
		}
	}
}

// namespaceDevice returns the namespace's single non-loopback interface, which
// is the guest end of the veth. The caller must already be inside the
// namespace.
func namespaceDevice() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var found string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if found != "" {
			return "", fmt.Errorf("netns: expected one non-loopback interface, found %s and %s", found, iface.Name)
		}
		found = iface.Name
	}
	if found == "" {
		return "", errors.New("netns: no non-loopback interface to filter on")
	}
	return found, nil
}

// installNetdevDenyTable adds a default-drop netdev egress chain on the guest
// veth.
//
// The inet output chain alone does not contain a sandbox. netfilter's IP hooks
// only see packets the kernel's own IP stack produced, and a userspace network
// stack — gVisor's, when run with --network=sandbox — does not use it. It
// writes Ethernet frames straight to the veth with AF_PACKET, below those
// hooks, so an output-chain policy filters traffic the sandbox never generates
// and every packet leaves the namespace regardless of policy. That was measured:
// with the output chain in place, TCP, UDP and ICMP to forbidden destinations
// all crossed the veth, and on a host whose FORWARD policy accepts (Docker's
// default drop is what happened to stop it) they reached the internet and were
// answered.
//
// The netdev egress hook sits at transmit, so it sees frames however they were
// produced. Verified against the same AF_PACKET injection: the send fails with
// ENOBUFS once this chain exists, and succeeds without it.
//
// See docs/engineering/gvisor-egress-finding-2026-09-04.md.
func (k *systemKernel) installNetdevDenyTable(ctx context.Context) error {
	device, err := namespaceDevice()
	if err != nil {
		return err
	}
	if err := k.nft(ctx, unix.NFT_MSG_NEWTABLE, unix.NLM_F_CREATE|unix.NLM_F_EXCL,
		unix.NFPROTO_NETDEV, concat(nlaString(unix.NFTA_TABLE_NAME, netdevTable), nlaU32BE(unix.NFTA_TABLE_FLAGS, 0))); err != nil {
		return fmt.Errorf("create netdev table: %w", err)
	}
	hook := nlaNestedAttr(unix.NFTA_CHAIN_HOOK, concat(
		nlaU32BE(unix.NFTA_HOOK_HOOKNUM, unix.NF_NETDEV_EGRESS),
		nlaU32BE(unix.NFTA_HOOK_PRIORITY, 0),
		nlaString(unix.NFTA_HOOK_DEV, device),
	))
	attrs := concat(
		nlaString(unix.NFTA_CHAIN_TABLE, netdevTable),
		nlaString(unix.NFTA_CHAIN_NAME, netdevChain),
		nlaString(unix.NFTA_CHAIN_TYPE, "filter"),
		hook,
		nlaU32BE(unix.NFTA_CHAIN_POLICY, nfDrop),
	)
	if err := k.nft(ctx, unix.NFT_MSG_NEWCHAIN, unix.NLM_F_CREATE|unix.NLM_F_EXCL, unix.NFPROTO_NETDEV, attrs); err != nil {
		return fmt.Errorf("create netdev egress chain on %s: %w", device, err)
	}
	return nil
}

func (k *systemKernel) installDenyTable(ctx context.Context) error {
	const table = "remount"
	if err := k.nft(ctx, unix.NFT_MSG_NEWTABLE, unix.NLM_F_CREATE|unix.NLM_F_EXCL,
		unix.NFPROTO_INET, concat(nlaString(unix.NFTA_TABLE_NAME, table), nlaU32BE(unix.NFTA_TABLE_FLAGS, 0))); err != nil {
		return fmt.Errorf("create nftables table: %w", err)
	}
	hook := nlaNestedAttr(unix.NFTA_CHAIN_HOOK, concat(
		nlaU32BE(unix.NFTA_HOOK_HOOKNUM, unix.NF_INET_LOCAL_OUT),
		nlaU32BE(unix.NFTA_HOOK_PRIORITY, 0),
	))
	attrs := concat(
		nlaString(unix.NFTA_CHAIN_TABLE, table),
		nlaString(unix.NFTA_CHAIN_NAME, "egress"),
		nlaString(unix.NFTA_CHAIN_TYPE, "filter"),
		hook,
		nlaU32BE(unix.NFTA_CHAIN_POLICY, nfDrop),
	)
	if err := k.nft(ctx, unix.NFT_MSG_NEWCHAIN, unix.NLM_F_CREATE|unix.NLM_F_EXCL, unix.NFPROTO_INET, attrs); err != nil {
		return fmt.Errorf("create nftables chain: %w", err)
	}
	forwardHook := nlaNestedAttr(unix.NFTA_CHAIN_HOOK, concat(
		nlaU32BE(unix.NFTA_HOOK_HOOKNUM, unix.NF_INET_FORWARD),
		nlaU32BE(unix.NFTA_HOOK_PRIORITY, 0),
	))
	forward := concat(
		nlaString(unix.NFTA_CHAIN_TABLE, table),
		nlaString(unix.NFTA_CHAIN_NAME, "forward"),
		nlaString(unix.NFTA_CHAIN_TYPE, "filter"),
		forwardHook,
		nlaU32BE(unix.NFTA_CHAIN_POLICY, nfDrop),
	)
	if err := k.nft(ctx, unix.NFT_MSG_NEWCHAIN, unix.NLM_F_CREATE|unix.NLM_F_EXCL, unix.NFPROTO_INET, forward); err != nil {
		return fmt.Errorf("create nftables forward chain: %w", err)
	}
	return nil
}

func (k *systemKernel) installForwardPermitRules(ctx context.Context, broker netip.AddrPort) error {
	for _, rule := range []struct {
		addressOffset uint32
		portOffset    uint32
		value         []byte
	}{
		{16, 2, broker.Addr().AsSlice()},
		{12, 0, broker.Addr().AsSlice()},
	} {
		expressions := concat(
			nftExpr("meta", concat(nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_NFPROTO), nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1))),
			nftCmp(unix.NFT_REG_1, []byte{unix.NFPROTO_IPV4}),
			nftExpr("payload", concat(nlaU32BE(unix.NFTA_PAYLOAD_BASE, unix.NFT_PAYLOAD_NETWORK_HEADER), nlaU32BE(unix.NFTA_PAYLOAD_OFFSET, rule.addressOffset), nlaU32BE(unix.NFTA_PAYLOAD_LEN, 4), nlaU32BE(unix.NFTA_PAYLOAD_DREG, unix.NFT_REG_1))),
			nftCmp(unix.NFT_REG_1, rule.value),
			nftExpr("meta", concat(nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_L4PROTO), nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1))),
			nftCmp(unix.NFT_REG_1, []byte{unix.IPPROTO_TCP}),
			nftExpr("payload", concat(nlaU32BE(unix.NFTA_PAYLOAD_BASE, unix.NFT_PAYLOAD_TRANSPORT_HEADER), nlaU32BE(unix.NFTA_PAYLOAD_OFFSET, rule.portOffset), nlaU32BE(unix.NFTA_PAYLOAD_LEN, 2), nlaU32BE(unix.NFTA_PAYLOAD_DREG, unix.NFT_REG_1))),
			nftCmp(unix.NFT_REG_1, []byte{byte(broker.Port() >> 8), byte(broker.Port())}),
			nftExpr("immediate", concat(nlaU32BE(unix.NFTA_IMMEDIATE_DREG, unix.NFT_REG_VERDICT), nlaNestedAttr(unix.NFTA_IMMEDIATE_DATA, nlaNestedAttr(unix.NFTA_DATA_VERDICT, nlaU32BE(unix.NFTA_VERDICT_CODE, nfAccept))))),
		)
		attrs := concat(nlaString(unix.NFTA_RULE_TABLE, "remount"), nlaString(unix.NFTA_RULE_CHAIN, "forward"), nlaNestedAttr(unix.NFTA_RULE_EXPRESSIONS, expressions))
		if err := k.nft(ctx, unix.NFT_MSG_NEWRULE, unix.NLM_F_CREATE|unix.NLM_F_APPEND, unix.NFPROTO_INET, attrs); err != nil {
			return err
		}
	}
	return nil
}

func (k *systemKernel) installPermitRule(ctx context.Context, broker netip.AddrPort) error {
	// The inet output chain defaults to drop. This sole accept rule matches
	// IPv4 + TCP + the generation-specific host-veth address and broker port.
	// IPv6, UDP, ICMP, raw packets and DNS therefore have no accepting path.
	expressions := concat(
		nftExpr("meta", concat(
			nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_NFPROTO),
			nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{unix.NFPROTO_IPV4}),
		nftExpr("payload", concat(
			nlaU32BE(unix.NFTA_PAYLOAD_BASE, unix.NFT_PAYLOAD_NETWORK_HEADER),
			nlaU32BE(unix.NFTA_PAYLOAD_OFFSET, 16),
			nlaU32BE(unix.NFTA_PAYLOAD_LEN, 4),
			nlaU32BE(unix.NFTA_PAYLOAD_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, broker.Addr().AsSlice()),
		nftExpr("meta", concat(
			nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_L4PROTO),
			nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{unix.IPPROTO_TCP}),
		nftExpr("payload", concat(
			nlaU32BE(unix.NFTA_PAYLOAD_BASE, unix.NFT_PAYLOAD_TRANSPORT_HEADER),
			nlaU32BE(unix.NFTA_PAYLOAD_OFFSET, 2),
			nlaU32BE(unix.NFTA_PAYLOAD_LEN, 2),
			nlaU32BE(unix.NFTA_PAYLOAD_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{byte(broker.Port() >> 8), byte(broker.Port())}),
		nftExpr("immediate", concat(
			nlaU32BE(unix.NFTA_IMMEDIATE_DREG, unix.NFT_REG_VERDICT),
			nlaNestedAttr(unix.NFTA_IMMEDIATE_DATA,
				nlaNestedAttr(unix.NFTA_DATA_VERDICT,
					nlaU32BE(unix.NFTA_VERDICT_CODE, nfAccept))),
		)),
	)
	attrs := concat(
		nlaString(unix.NFTA_RULE_TABLE, "remount"),
		nlaString(unix.NFTA_RULE_CHAIN, "egress"),
		nlaNestedAttr(unix.NFTA_RULE_EXPRESSIONS, expressions),
	)
	if err := k.nft(ctx, unix.NFT_MSG_NEWRULE, unix.NLM_F_CREATE|unix.NLM_F_APPEND, unix.NFPROTO_INET, attrs); err != nil {
		return err
	}
	// The same single exception on the netdev egress chain. Without it that
	// chain's drop policy would also stop the broker, which is the one
	// destination a workspace is allowed to reach.
	//
	// The match cannot be reused verbatim. A netdev chain sees the frame, not a
	// routed packet, so "meta nfproto" — which the inet chain uses to select
	// IPv4 — is not set there. The equivalent at this layer is the ethertype,
	// "meta protocol" == 0x0800. Everything after it is identical, because the
	// network and transport header offsets resolve the same way.
	netdevExpressions := concat(
		nftExpr("meta", concat(
			nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_PROTOCOL),
			nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{0x08, 0x00}),
		nftExpr("payload", concat(
			nlaU32BE(unix.NFTA_PAYLOAD_BASE, unix.NFT_PAYLOAD_NETWORK_HEADER),
			nlaU32BE(unix.NFTA_PAYLOAD_OFFSET, 16),
			nlaU32BE(unix.NFTA_PAYLOAD_LEN, 4),
			nlaU32BE(unix.NFTA_PAYLOAD_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, broker.Addr().AsSlice()),
		nftExpr("meta", concat(
			nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_L4PROTO),
			nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{unix.IPPROTO_TCP}),
		nftExpr("payload", concat(
			nlaU32BE(unix.NFTA_PAYLOAD_BASE, unix.NFT_PAYLOAD_TRANSPORT_HEADER),
			nlaU32BE(unix.NFTA_PAYLOAD_OFFSET, 2),
			nlaU32BE(unix.NFTA_PAYLOAD_LEN, 2),
			nlaU32BE(unix.NFTA_PAYLOAD_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{byte(broker.Port() >> 8), byte(broker.Port())}),
		nftExpr("immediate", concat(
			nlaU32BE(unix.NFTA_IMMEDIATE_DREG, unix.NFT_REG_VERDICT),
			nlaNestedAttr(unix.NFTA_IMMEDIATE_DATA,
				nlaNestedAttr(unix.NFTA_DATA_VERDICT,
					nlaU32BE(unix.NFTA_VERDICT_CODE, nfAccept))),
		)),
	)
	netdevAttrs := concat(
		nlaString(unix.NFTA_RULE_TABLE, netdevTable),
		nlaString(unix.NFTA_RULE_CHAIN, netdevChain),
		nlaNestedAttr(unix.NFTA_RULE_EXPRESSIONS, netdevExpressions),
	)
	if err := k.nft(ctx, unix.NFT_MSG_NEWRULE, unix.NLM_F_CREATE|unix.NLM_F_APPEND, unix.NFPROTO_NETDEV, netdevAttrs); err != nil {
		return err
	}
	// ARP has to be allowed, and only a netdev chain ever had to care. An inet
	// output chain never sees ARP because ARP is not IP, so the existing policy
	// was silently unaffected by it. A netdev chain sees every frame, so a
	// drop policy that only excepts IPv4 also drops the sandbox's ARP request
	// for the host's MAC — after which nothing can be delivered at all, and the
	// symptom is that even the permitted broker is unreachable.
	//
	// This is not an egress path. The device is one end of a veth pair whose
	// only peer is this workspace's host side, so an ARP frame can reach
	// nothing else, and the IPv4 rule above still constrains everything that
	// ARP would resolve a route for. IPv6 needs no equivalent: InstallDenyAll
	// disables it in this namespace outright.
	arpExpressions := concat(
		nftExpr("meta", concat(
			nlaU32BE(unix.NFTA_META_KEY, unix.NFT_META_PROTOCOL),
			nlaU32BE(unix.NFTA_META_DREG, unix.NFT_REG_1),
		)),
		nftCmp(unix.NFT_REG_1, []byte{0x08, 0x06}),
		nftExpr("immediate", concat(
			nlaU32BE(unix.NFTA_IMMEDIATE_DREG, unix.NFT_REG_VERDICT),
			nlaNestedAttr(unix.NFTA_IMMEDIATE_DATA,
				nlaNestedAttr(unix.NFTA_DATA_VERDICT,
					nlaU32BE(unix.NFTA_VERDICT_CODE, nfAccept))),
		)),
	)
	arpAttrs := concat(
		nlaString(unix.NFTA_RULE_TABLE, netdevTable),
		nlaString(unix.NFTA_RULE_CHAIN, netdevChain),
		nlaNestedAttr(unix.NFTA_RULE_EXPRESSIONS, arpExpressions),
	)
	return k.nft(ctx, unix.NFT_MSG_NEWRULE, unix.NLM_F_CREATE|unix.NLM_F_APPEND, unix.NFPROTO_NETDEV, arpAttrs)
}

func (k *systemKernel) nft(ctx context.Context, message uint16, flags uint16, family byte, attrs []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_NETFILTER)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1}); err != nil {
		return err
	}
	endSeq := k.seq.Add(3)
	beginSeq := endSeq - 2
	operationSeq := endSeq - 1
	nfgen := func(family byte, resource uint16) []byte {
		return []byte{family, unix.NFNETLINK_V0, byte(resource >> 8), byte(resource)}
	}
	packet := concat(
		netlinkPacket(unix.NFNL_MSG_BATCH_BEGIN, unix.NLM_F_REQUEST, beginSeq, nfgen(unix.AF_UNSPEC, unix.NFNL_SUBSYS_NFTABLES)),
		netlinkPacket(uint16(unix.NFNL_SUBSYS_NFTABLES<<8)|message, unix.NLM_F_REQUEST|unix.NLM_F_ACK|flags, operationSeq, append(nfgen(family, 0), attrs...)),
		netlinkPacket(unix.NFNL_MSG_BATCH_END, unix.NLM_F_REQUEST, endSeq, nfgen(unix.AF_UNSPEC, unix.NFNL_SUBSYS_NFTABLES)),
	)
	if err := unix.Sendto(fd, packet, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return err
	}
	buf := make([]byte, 64<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				continue
			}
			return err
		}
		messages, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return err
		}
		for _, reply := range messages {
			if reply.Header.Seq != operationSeq || reply.Header.Type != unix.NLMSG_ERROR {
				continue
			}
			if len(reply.Data) < 4 {
				return errors.New("short nftables acknowledgement")
			}
			code := int32(nativeEndian.Uint32(reply.Data[:4]))
			if code == 0 {
				return nil
			}
			return unix.Errno(-code)
		}
	}
}

func netlinkPacket(typ, flags uint16, seq uint32, body []byte) []byte {
	header := unix.NlMsghdr{Len: uint32(unix.NLMSG_HDRLEN + len(body)), Type: typ, Flags: flags, Seq: seq}
	return append(marshal(header), body...)
}

func nftExpr(name string, data []byte) []byte {
	return nlaNestedAttr(unix.NFTA_LIST_ELEM, concat(
		nlaString(unix.NFTA_EXPR_NAME, name),
		nlaNestedAttr(unix.NFTA_EXPR_DATA, data),
	))
}

func nftCmp(register uint32, value []byte) []byte {
	return nftExpr("cmp", concat(
		nlaU32BE(unix.NFTA_CMP_SREG, register),
		nlaU32BE(unix.NFTA_CMP_OP, unix.NFT_CMP_EQ),
		nlaNestedAttr(unix.NFTA_CMP_DATA, nlaBytes(unix.NFTA_DATA_VALUE, value)),
	))
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

var nativeEndian = binary.NativeEndian

func marshal(v any) []byte {
	var b bytes.Buffer
	_ = binary.Write(&b, nativeEndian, v)
	return b.Bytes()
}

func nlaBytes(typ uint16, value []byte) []byte {
	length := unix.NLA_HDRLEN + len(value)
	out := make([]byte, (length+3)&^3)
	nativeEndian.PutUint16(out[0:2], uint16(length))
	nativeEndian.PutUint16(out[2:4], typ)
	copy(out[unix.NLA_HDRLEN:], value)
	return out
}

func nlaString(typ uint16, value string) []byte { return nlaBytes(typ, append([]byte(value), 0)) }
func nlaU32Native(typ uint16, value uint32) []byte {
	b := make([]byte, 4)
	nativeEndian.PutUint32(b, value)
	return nlaBytes(typ, b)
}
func nlaU32BE(typ uint16, value uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, value)
	return nlaBytes(typ, b)
}
func nlaNestedAttr(typ uint16, value []byte) []byte { return nlaBytes(typ|nlaNested, value) }
