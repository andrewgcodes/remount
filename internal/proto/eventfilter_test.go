package proto

import "testing"

func egressEvent(t *testing.T, eventType, binding, host string) Event {
	t.Helper()
	return Event{Type: eventType, Payload: MustMarshal(map[string]any{
		"binding": binding, "host": host, "decision": "substituted",
	})}
}

func TestEventFilterNarrowsByBindingHostAndType(t *testing.T) {
	used := egressEvent(t, EvCredUsed, "b_openai", "api.openai.com:443")
	denied := egressEvent(t, EvEgressDenied, "b_github", "api.github.com:443")
	moved := Event{Type: EvWSMoved, Payload: MustMarshal(map[string]any{"node": "n1"})}
	bare := Event{Type: EvNodeOnline}

	cases := []struct {
		name   string
		filter EventFilter
		event  Event
		want   bool
	}{
		{"empty matches everything", EventFilter{}, moved, true},
		{"empty matches a payloadless event", EventFilter{}, bare, true},
		{"binding matches", EventFilter{Binding: "b_openai"}, used, true},
		{"binding excludes another binding", EventFilter{Binding: "b_openai"}, denied, false},
		{"binding excludes an event with no binding", EventFilter{Binding: "b_openai"}, moved, false},
		{"binding excludes a payloadless event", EventFilter{Binding: "b_openai"}, bare, false},
		{"host matches the recorded authority", EventFilter{Host: "api.openai.com:443"}, used, true},
		{"a bare host matches its authority", EventFilter{Host: "api.openai.com"}, used, true},
		{"host is case insensitive", EventFilter{Host: "API.OpenAI.com"}, used, true},
		{"host excludes another destination", EventFilter{Host: "api.openai.com"}, denied, false},
		{"type prefix selects a family", EventFilter{Types: []string{"egress"}}, denied, true},
		{"type prefix excludes another family", EventFilter{Types: []string{"egress"}}, used, false},
		{"an exact type still matches", EventFilter{Types: []string{EvCredUsed}}, used, true},
		{"several prefixes are a union", EventFilter{Types: []string{"egress", "cred"}}, used, true},
		{"fields are conjunctive", EventFilter{Types: []string{"cred"}, Binding: "b_github"}, used, false},
		{"every field agreeing matches", EventFilter{Types: []string{"cred"}, Binding: "b_openai", Host: "api.openai.com"}, used, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.filter.Match(tc.event); got != tc.want {
				t.Fatalf("Match = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEventsTailReqCarriesItsFilter(t *testing.T) {
	req := EventsTailReq{From: 3, WS: "ws_1", Binding: "b_x", Host: "example.test", Types: []string{"egress"}}
	filter := req.Filter()
	if filter.Empty() {
		t.Fatal("a request that declares filters reported an empty one")
	}
	if filter.Binding != "b_x" || filter.Host != "example.test" || len(filter.Types) != 1 {
		t.Fatalf("filter = %+v", filter)
	}
	// A request that declares nothing must not narrow anything: an older peer
	// still receives the stream it asked for.
	if !(EventsTailReq{From: 3, WS: "ws_1"}).Filter().Empty() {
		t.Fatal("an unfiltered request produced a narrowing filter")
	}
}
