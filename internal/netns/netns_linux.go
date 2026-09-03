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

	nfDrop   = 0
	nfAccept = 1
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
	if filepath.Dir(filepath.Clean(namespace)) != "/run/remount/netns" {
		return fmt.Errorf("namespace %q is outside /run/remount/netns", namespace)
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
	dir := "/run/remount/netns"
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
		return k.installDenyTable(ctx)
	})
}

func (k *systemKernel) PermitBroker(ctx context.Context, namespace string, broker netip.AddrPort) error {
	return k.withNamespace(namespace, func() error { return k.installPermitRule(ctx, broker) })
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
	return k.nft(ctx, unix.NFT_MSG_NEWRULE, unix.NLM_F_CREATE|unix.NLM_F_APPEND, unix.NFPROTO_INET, attrs)
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
