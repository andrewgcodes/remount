#!/usr/bin/env bash
set -euo pipefail

# Proves the kernel/runsc mechanism before the Go backend is enabled. This is
# intentionally a host-gated spike; production code uses x/sys netlink and
# never shells out to iptables, ip, or nft.

if [[ $(id -u) -ne 0 ]]; then
  echo "unavailable: gvisor spike requires root" >&2
  exit 77
fi

for binary in ip mountpoint nft runsc setsid socat umount; do
  if ! command -v "$binary" >/dev/null; then
    echo "unavailable: $binary is required" >&2
    exit 77
  fi
done

rootfs=${REMOUNT_GVISOR_ROOTFS:-}
if [[ -z $rootfs || ! -d $rootfs ]]; then
  echo "unavailable: REMOUNT_GVISOR_ROOTFS must name an unpacked rootfs" >&2
  exit 77
fi

spike_dir=$(mktemp -d /tmp/remount-gvisor-spike.XXXXXX)
suffix=${spike_dir##*.}
namespace=rmspike-$suffix
host_if=rmh-${suffix:0:6}
guest_if=rmg-${suffix:0:6}
container=remount-spike-$suffix
host_table=rmspike${suffix}
state=$spike_dir/state
bundle=$spike_dir/bundle
work=$bundle/work
broker_pid=
udp_pid=

cleanup() {
  runsc --root="$state" delete --force "$container" >/dev/null 2>&1 || true
  for pid in "$broker_pid" "$udp_pid"; do
    if [[ -n $pid ]]; then
      kill -- "-$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
      kill -KILL -- "-$pid" >/dev/null 2>&1 || true
    fi
  done
  ip link delete "$host_if" >/dev/null 2>&1 || true
  nft delete table netdev "$host_table" >/dev/null 2>&1 || true
  ip netns delete "$namespace" >/dev/null 2>&1 || true
  if mountpoint -q "$state/null-netns"; then umount "$state/null-netns"; fi
  rm -rf -- "$spike_dir"
}
trap cleanup EXIT

mkdir -p "$state" "$work"
ip netns add "$namespace"
ip link add "$host_if" type veth peer name "$guest_if"
ip link set "$guest_if" netns "$namespace"
ip addr add 169.254.251.1/30 dev "$host_if"
ip netns exec "$namespace" ip addr add 169.254.251.2/30 dev "$guest_if"
ip netns exec "$namespace" sh -c 'echo 1 > /proc/sys/net/ipv6/conf/all/disable_ipv6'
ip netns exec "$namespace" nft -f - <<'NFT'
table inet remount {
  chain egress {
    type filter hook output priority filter; policy drop;
    ip daddr 169.254.251.1 tcp dport 17443 accept
  }
}
NFT
ip netns exec "$namespace" nft -f - <<NFT
table netdev remount {
  chain egress {
    type filter hook egress device "$guest_if" priority 0; policy drop;
    ether type arp accept
    ip daddr 169.254.251.1 tcp dport 17443 accept
    counter drop
  }
}
NFT

# The guest endpoint must be up before Linux accepts its gateway. The host
# endpoint stays down until the sandbox is running, so no traffic can cross.
ip netns exec "$namespace" ip link set "$guest_if" up
ip netns exec "$namespace" ip route add default via 169.254.251.1

setsid socat TCP4-LISTEN:17443,bind=169.254.251.1,reuseaddr,fork EXEC:/bin/cat >/dev/null 2>&1 &
broker_pid=$!
setsid socat UDP4-LISTEN:17444,bind=169.254.251.1,reuseaddr,fork EXEC:/bin/cat >/dev/null 2>&1 &
udp_pid=$!

# Start runsc while the host veth endpoint is down and after deny-all is
# committed. This eliminates an unfiltered startup interval.
cat >"$bundle/config.json" <<JSON
{
  "ociVersion": "1.0.2",
  "process": {
    "terminal": false,
    "user": {"uid": 0, "gid": 0},
    "args": ["sleep", "infinity"],
    "env": ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/work"],
    "cwd": "/work",
    "noNewPrivileges": true
  },
  "root": {"path": "$rootfs", "readonly": true},
  "hostname": "remount-spike",
  "mounts": [
    {"destination": "/proc", "type": "proc", "source": "proc"},
    {"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "mode=755", "size=65536k"]},
    {"destination": "/work", "type": "bind", "source": "$work", "options": ["rbind", "rw"]}
  ],
  "linux": {"namespaces": [
    {"type": "pid"}, {"type": "ipc"}, {"type": "uts"}, {"type": "mount"},
    {"type": "network", "path": "/var/run/netns/$namespace"}
  ]}
}
JSON

runsc --root="$state" --network=sandbox --net-raw=false --allow-packet-socket-write=false create --bundle="$bundle" "$container" >"$state/create.log" 2>&1
runsc --root="$state" start "$container" >"$state/start.log" 2>&1
ip link set "$host_if" up
ip netns exec "$namespace" ip link set lo up

inside() { runsc --root="$state" exec "$container" /bin/sh -c "$1"; }
deny() {
  local name=$1 command=$2
  if inside "$command" >/dev/null 2>&1; then
    echo "FAIL: $name unexpectedly succeeded" >&2
    exit 1
  fi
  echo "PASS: $name denied"
}
drop_packets() {
  ip netns exec "$namespace" nft list chain netdev remount egress |
    awk '/counter packets/ { print $3; exit }'
}
deny_connectionless() {
  local name=$1 command=$2 before after
  before=$(drop_packets)
  inside "$command" >/dev/null 2>&1 || true
  after=$(drop_packets)
  if ((after <= before)); then
    echo "FAIL: $name did not reach the deny-first policy" >&2
    exit 1
  fi
  echo "PASS: $name denied"
}

inside 'nc -z -w 2 169.254.251.1 17443'
echo "PASS: broker reachable"
"$rootfs/udpprobe" 169.254.251.1 17444
echo "PASS: UDP probe positive control"
deny "direct IPv4 TCP" 'nc -z -w 2 1.1.1.1 443'
deny "IPv6" 'nc -z -w 2 2606:4700:4700::1111 443'
deny "UDP" '/udpprobe 169.254.251.1 17444'
deny_connectionless "UDP" 'nc -u -z -w 2 8.8.8.8 53'
deny "DNS" 'nslookup example.com 8.8.8.8'
deny "ICMP" 'ping -c 1 -W 2 8.8.8.8'
deny "raw socket" '/rawprobe'
deny "CONNECT to unlisted host" "printf 'CONNECT unlisted.example:443 HTTP/1.1\\r\\n\\r\\n' | nc -w 2 203.0.113.1 443"

ip link delete "$host_if"
deny "broker after revoke" 'nc -z -w 2 169.254.251.1 17443'
echo "gVisor E4 spike passed"
