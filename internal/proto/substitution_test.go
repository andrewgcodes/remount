package proto

import "testing"

func TestSubstitutionLocationDefaultsToHeader(t *testing.T) {
	if got := SubstitutionLocation(BindingLease{ID: "b_x"}); got != SubstitutionHeader {
		t.Fatalf("a binding declaring nothing = %q, want %q", got, SubstitutionHeader)
	}
	if got := SubstitutionLocation(BindingLease{Substitution: &BindingSubstitution{}}); got != SubstitutionHeader {
		t.Fatalf("an empty location = %q, want %q", got, SubstitutionHeader)
	}
	lease := BindingLease{Substitution: &BindingSubstitution{Location: SubstitutionBodyJSON, JSONPointer: "/a"}}
	if got := SubstitutionLocation(lease); got != SubstitutionBodyJSON {
		t.Fatalf("declared location = %q", got)
	}
}

func TestValidSubstitutionRefusesUnusableDeclarations(t *testing.T) {
	cases := []struct {
		name string
		in   *BindingSubstitution
		want bool
	}{
		{"nil is the header default", nil, true},
		{"explicit header", &BindingSubstitution{Location: SubstitutionHeader}, true},
		{"header with a name is contradictory", &BindingSubstitution{Location: SubstitutionHeader, Name: "x"}, false},
		{"query needs a name", &BindingSubstitution{Location: SubstitutionQuery}, false},
		{"query with a name", &BindingSubstitution{Location: SubstitutionQuery, Name: "api_key"}, true},
		{"query with a pointer is contradictory", &BindingSubstitution{Location: SubstitutionQuery, Name: "k", JSONPointer: "/a"}, false},
		{"form needs a name", &BindingSubstitution{Location: SubstitutionBodyForm}, false},
		{"form with a name", &BindingSubstitution{Location: SubstitutionBodyForm, Name: "client_secret"}, true},
		{"json needs a pointer", &BindingSubstitution{Location: SubstitutionBodyJSON}, false},
		{"json pointer must be absolute", &BindingSubstitution{Location: SubstitutionBodyJSON, JSONPointer: "auth/token"}, false},
		{"json with an absolute pointer", &BindingSubstitution{Location: SubstitutionBodyJSON, JSONPointer: "/auth/token"}, true},
		{"unknown location", &BindingSubstitution{Location: "cookie", Name: "s"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ValidSubstitution(tc.in); got != tc.want {
				t.Fatalf("ValidSubstitution(%+v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// A node decodes the lease from CBOR, so the new field has to survive the
// wire. A peer that predates it must still decode the rest of the lease.
func TestBindingLeaseSubstitutionRoundTrips(t *testing.T) {
	lease := BindingLease{
		ID: "b_x", Secret: "synthetic-test-secret", Destinations: []string{"api.example"},
		Substitution: &BindingSubstitution{Location: SubstitutionBodyJSON, JSONPointer: "/auth/token"},
	}
	var decoded BindingLease
	if err := Unmarshal(MustMarshal(lease), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Substitution == nil || *decoded.Substitution != *lease.Substitution {
		t.Fatalf("decoded substitution = %+v", decoded.Substitution)
	}

	// Omitted, it stays omitted rather than decoding as an empty declaration
	// that would look like an explicit header choice with no fields.
	plain := BindingLease{ID: "b_y", Secret: "synthetic-test-secret", Destinations: []string{"api.example"}}
	var plainDecoded BindingLease
	if err := Unmarshal(MustMarshal(plain), &plainDecoded); err != nil {
		t.Fatal(err)
	}
	if plainDecoded.Substitution != nil {
		t.Fatalf("an undeclared substitution decoded as %+v", plainDecoded.Substitution)
	}
	if SubstitutionLocation(plainDecoded) != SubstitutionHeader {
		t.Fatal("an undeclared substitution did not resolve to the header default")
	}
}
