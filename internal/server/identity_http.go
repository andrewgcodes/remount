package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/proto"
)

type oidcExchangeRequest struct {
	Tenant  string `json:"tenant"`
	IDToken string `json:"id_token"`
	Nonce   string `json:"nonce"`
}

type tokenPairResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

func (s *Server) identityRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/identity/oidc", s.handleOIDCConfig)
	mux.HandleFunc("POST /v1/identity/oidc/exchange", s.handleOIDCExchange)
	mux.HandleFunc("POST /v1/identity/refresh", s.handleIdentityRefresh)
}

func (s *Server) oidcConfig(r *http.Request, tenantID string) (proto.TenantOIDC, error) {
	if s.Tenants == nil || s.Identity == nil || tenantID == "" || tenantID == "*" {
		return proto.TenantOIDC{}, proto.Err(proto.CodeNotFound, "OIDC tenant is not configured")
	}
	tenantValue, err := s.Tenants.Get(r.Context(), tenantID)
	if err != nil || tenantValue.Policy.OIDC.Issuer == "" {
		return proto.TenantOIDC{}, proto.Err(proto.CodeNotFound, "OIDC tenant is not configured")
	}
	value := tenantValue.Policy.OIDC
	return proto.TenantOIDC{Issuer: value.Issuer, ClientID: value.ClientID, Audience: value.Audience,
		Scopes: append([]string(nil), value.Scopes...), GroupRoles: cloneHTTPRoleMap(value.GroupRoles),
		TenantClaim: value.TenantClaim, GroupsClaim: value.GroupsClaim}, nil
}

func (s *Server) handleOIDCConfig(w http.ResponseWriter, r *http.Request) {
	config, err := s.oidcConfig(r, r.URL.Query().Get("tenant"))
	if err != nil {
		writeError(w, err)
		return
	}
	// Group-to-role policy is server authority and is not needed by the public
	// device client. Keeping it server-side also limits tenant metadata leaks.
	config.GroupRoles = nil
	writeJSON(w, http.StatusOK, config)
}

func (s *Server) handleOIDCExchange(w http.ResponseWriter, r *http.Request) {
	var request oidcExchangeRequest
	if err := decodeIdentityJSON(r, &request); err != nil {
		badRequest(w, "%v", err)
		return
	}
	config, err := s.oidcConfig(r, request.Tenant)
	if err != nil {
		writeError(w, err)
		return
	}
	client, err := identity.NewOIDCClient(identity.OIDCOptions{Issuer: config.Issuer, ClientID: config.ClientID,
		Audience: config.Audience, Scopes: config.Scopes, GroupRoles: config.GroupRoles})
	if err != nil {
		writeError(w, proto.Err(proto.CodeUnreachable, "OIDC configuration is unavailable"))
		return
	}
	discovery, err := client.Discover(r.Context())
	if err != nil {
		writeError(w, proto.Err(proto.CodeUnreachable, "OIDC discovery is unavailable"))
		return
	}
	verifier, err := identity.NewJWKSVerifier(identity.JWKSOptions{JWKSURI: discovery.JWKSURI, TenantClaim: config.TenantClaim, GroupsClaim: config.GroupsClaim})
	if err != nil {
		writeError(w, proto.Err(proto.CodeUnreachable, "OIDC verifier is unavailable"))
		return
	}
	access, refresh, err := client.ExchangeForTenant(r.Context(), s.Identity, verifier, request.IDToken, request.Nonce, request.Tenant)
	if err != nil {
		writeError(w, proto.Err(proto.CodeUnauthorized, "OIDC identity was refused"))
		return
	}
	writeJSON(w, http.StatusOK, tokenPairResponse{AccessToken: access, RefreshToken: refresh})
}

func (s *Server) handleIdentityRefresh(w http.ResponseWriter, r *http.Request) {
	if s.Identity == nil {
		writeError(w, proto.Err(proto.CodeNotFound, "identity endpoint is unavailable"))
		return
	}
	var request struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := decodeIdentityJSON(r, &request); err != nil {
		badRequest(w, "%v", err)
		return
	}
	access, refresh, err := s.Identity.RotateRefresh(r.Context(), request.RefreshToken)
	if err != nil {
		writeError(w, proto.Err(proto.CodeUnauthorized, "refresh credential was refused"))
		return
	}
	writeJSON(w, http.StatusOK, tokenPairResponse{AccessToken: access, RefreshToken: refresh})
}

func decodeIdentityJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, apiBodyLimit+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid JSON body")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON body")
	}
	return nil
}

func cloneHTTPRoleMap(in map[string][]string) map[string][]string {
	out := make(map[string][]string, len(in))
	for key, roles := range in {
		out[key] = append([]string(nil), roles...)
	}
	return out
}
