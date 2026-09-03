package server

import (
	"context"
	"errors"
	"time"

	"remount.dev/remount/internal/identity"
)

// BootstrapOperator creates the first global operator and returns one
// short-lived access bearer. It is intentionally unavailable outside the
// built-in production identity runtime and refuses an existing principal.
func (s *Server) BootstrapOperator(ctx context.Context, principal string, ttl time.Duration) (string, time.Time, error) {
	if s.opts.Mode == ModeStandalone || s.Identity == nil {
		return "", time.Time{}, errors.New("server: operator bootstrap requires built-in production identity")
	}
	if _, err := s.Identity.CreatePrincipal(ctx, "*", principal, []string{identity.RoleOperator}, "bootstrap"); err != nil {
		return "", time.Time{}, err
	}
	return s.Identity.IssueAccessToken(ctx, "*", principal, identity.RoleOperator, ttl)
}
