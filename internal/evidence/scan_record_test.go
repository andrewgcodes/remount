package evidence

import (
	"strings"
	"testing"
)

// The rule is a property of the record, not of one field: a record that names
// a variable anywhere and carries that variable's value anywhere carries the
// value of a variable it names. Collecting the names per field lets a record
// split the two halves across two fields and scan clean.
//
// `remount evidence validate` calls ScanSecrets directly on a record file, so
// a hole here is a hole in the only check that stands between a hand-written
// record and a committed credential.
func TestARecordCannotSplitANameFromItsValueAcrossFields(t *testing.T) {
	const value = "planted-deploy-value-98765"
	lookup := fakeEnv(map[string]string{"REMOUNT_DEPLOY_TOKEN": value})

	rec := validRecord()
	rec.Status = StatusFailed
	// The variable is named in one field...
	rec.Owner = "scripts/deploy.sh, which reads REMOUNT_DEPLOY_TOKEN"
	// ...and its value is carried in another, which never names it.
	rec.Reason = "the upstream refused " + value

	findings := rec.ScanSecrets(lookup)
	if len(findings) == 0 {
		t.Fatal("a record that named a variable in one field and carried its value in another scanned clean")
	}
	var named bool
	for _, f := range findings {
		if f.Rule == "env-value:REMOUNT_DEPLOY_TOKEN" {
			named = true
		}
		if strings.Contains(f.String(), value) {
			t.Fatalf("the finding reproduced the secret: %s", f)
		}
	}
	if !named {
		t.Fatalf("no finding named the variable, got %v", findings)
	}
}

// The whole-record collection must not turn every record into a false
// positive: naming a variable is still allowed, and a value the record never
// carries is still not a leak.
func TestNamingAVariableInOneFieldIsStillAllowed(t *testing.T) {
	lookup := fakeEnv(map[string]string{"REMOUNT_DEPLOY_TOKEN": "planted-deploy-value-98765"})
	rec := validRecord()
	rec.Owner = "scripts/deploy.sh, which reads REMOUNT_DEPLOY_TOKEN"
	rec.RequiredEnv = []string{"REMOUNT_DEPLOY_TOKEN"}
	if findings := rec.ScanSecrets(lookup); len(findings) != 0 {
		t.Fatalf("naming a variable is allowed, got %v", findings)
	}
}
