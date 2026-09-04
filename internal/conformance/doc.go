// Package conformance is the executable form of spec/PROTOCOL.md: a versioned
// manifest of semantic requirements, a black-box runner that exercises them
// against any implementation, and the report formats Plan B §6 requires.
//
// # The one rule
//
// Nothing under this package may import a Remount implementation package.
// Every assertion observes only what a foreign implementation also publishes:
// the frame protocol at /v1/link, the HTTP surface, the event log, the
// diagnostic endpoints, and (optionally) a command-line client. The wire
// types in wire.go are therefore declared here a second time, from the spec,
// rather than imported from internal/proto — a decoder that shares its
// definitions with the encoder under test proves nothing about the encoding.
//
// The same reasoning explains why Record in report.go duplicates
// internal/evidence.Record instead of importing it: that package is an
// implementation package of this repository, and importing it would make the
// conformance suite unusable against any other implementation. The struct is
// small and its field names are fixed by Plan B §6, so the duplication is
// cheap and the drift is caught by report_test.go, which compares this
// package's JSON keys against the documented schema.
//
// # Layout
//
//	manifest.go   the versioned requirement manifest and its invariants
//	wire.go       an independent frame codec and relay client
//	target.go     the three target modes of Plan B §12.2
//	session.go    the observation surfaces one run is given
//	checks*.go    one function per requirement, grouped by semantic category
//	runner.go     manifest x target -> results
//	report.go     JUnit XML and the Plan B §6 evidence record
//	shim/         a minimal implementation with switchable defects (B21)
package conformance
