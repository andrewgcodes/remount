package proto

import "testing"

func TestNormalizeArtifactFormatFirecrackerFullV1(t *testing.T) {
	got, err := NormalizeArtifactFormat(ArtifactFormatFirecrackerFullV1)
	if err != nil || got != ArtifactFormatFirecrackerFullV1 {
		t.Fatalf("normalize = %q, %v", got, err)
	}
}
