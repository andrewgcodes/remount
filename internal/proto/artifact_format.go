package proto

// NormalizeArtifactFormat maps the pre-format wire representation to the
// legacy tar format and rejects representations this release cannot restore.
func NormalizeArtifactFormat(format string) (string, error) {
	switch format {
	case "", ArtifactFormatTar:
		return ArtifactFormatTar, nil
	case ArtifactFormatChunkedV1, ArtifactFormatFirecrackerFullV1:
		return format, nil
	default:
		return "", Err(CodeUnsupported, "artifact format %q is unsupported", format)
	}
}
