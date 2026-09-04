//go:build linux

package netns

import (
	"net/netip"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestGuestEgressRulesPermitOnlyBrokerTCP(t *testing.T) {
	deny := denyEgressRules()
	if want := []tcFlowerRule{{priority: 100, protocol: unix.ETH_P_ALL, action: tcActionDrop}}; !reflect.DeepEqual(deny, want) {
		t.Fatalf("deny rules = %+v, want %+v", deny, want)
	}
	broker := netip.MustParseAddrPort("169.254.1.1:17443")
	permit := brokerEgressRules(broker)
	want := []tcFlowerRule{
		{priority: 1, protocol: unix.ETH_P_ARP, action: tcActionPass},
		{
			priority:        2,
			protocol:        unix.ETH_P_IP,
			action:          tcActionPass,
			destination:     broker.Addr(),
			ipProtocol:      unix.IPPROTO_TCP,
			destinationPort: broker.Port(),
		},
	}
	if !reflect.DeepEqual(permit, want) {
		t.Fatalf("permit rules = %+v, want %+v", permit, want)
	}
}
