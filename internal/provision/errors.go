package provision

import (
	"errors"
	"strings"
)

// ErrUnavailable means a provider cannot honestly satisfy the requested
// lifecycle operation with its configured API, helper, or feature set.
var ErrUnavailable = errors.New("provision: provider unavailable")

// PoolLabel is the reserved machine label used by pool reconciliation.
const PoolLabel = "remount.pool"

// NodeLabel binds provider inventory to the exact Remount node identity that
// reports assignments to the control plane. Scale-down must never infer this
// relationship from list ordering or machine names.
const NodeLabel = "remount.node"

// ValidateSecretBoundary rejects an enrollment token copied into any
// provider-visible identity or placement field. Its error is deliberately
// value-free so malformed input cannot make the credential observable.
func ValidateSecretBoundary(request Request) error {
	token := request.Bootstrap.EnrollmentToken
	if token == "" {
		return nil
	}
	visible := []string{request.Name, request.Tenant, request.Region, request.Size}
	for key, value := range request.Labels {
		visible = append(visible, key, value)
	}
	for _, value := range visible {
		if strings.Contains(value, token) {
			return errors.New("provision: enrollment token appears in provider-visible metadata")
		}
	}
	return nil
}
