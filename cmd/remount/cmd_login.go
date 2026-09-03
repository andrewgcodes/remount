package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"remount.dev/remount/internal/identity"
	"remount.dev/remount/internal/proto"
)

type credentialFile struct {
	Server       string `json:"server"`
	Tenant       string `json:"tenant"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	UpdatedAt    int64  `json:"updated_at"`
}

func defaultCredentialPath() string {
	if configured := os.Getenv("REMOUNT_CREDENTIAL_FILE"); configured != "" {
		return configured
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".remount", "credentials.json")
}

func storedAccessToken(server string) string {
	path := defaultCredentialPath()
	if path == "" {
		return ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var stored credentialFile
	if json.Unmarshal(raw, &stored) != nil || stored.Server != server {
		return ""
	}
	if accessTokenExpiresSoon(stored.AccessToken, time.Now().Add(5*time.Minute)) {
		if refreshed, err := rotateStoredCredential(server, path, stored); err == nil {
			stored = refreshed
		}
	}
	return stored.AccessToken
}

func accessTokenExpiresSoon(token string, threshold time.Time) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Expires int64 `json:"exp"`
	}
	return json.Unmarshal(payload, &claims) == nil && claims.Expires != 0 && !time.Unix(claims.Expires, 0).After(threshold)
}

func rotateStoredCredential(server, path string, stored credentialFile) (credentialFile, error) {
	if stored.RefreshToken == "" {
		return stored, errors.New("stored refresh credential is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var tokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := postLoginJSON(ctx, client, strings.TrimSuffix(server, "/")+"/v1/identity/refresh", map[string]string{"refresh_token": stored.RefreshToken}, &tokens); err != nil {
		return stored, err
	}
	if tokens.AccessToken == "" || tokens.RefreshToken == "" {
		return stored, errors.New("refresh returned incomplete credentials")
	}
	stored.AccessToken, stored.RefreshToken, stored.UpdatedAt = tokens.AccessToken, tokens.RefreshToken, time.Now().UnixMilli()
	return stored, writeCredentialFile(path, stored)
}

func cmdLogin(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	serverURL := fs.String("server", envOr("REMOUNT_SERVER", "http://127.0.0.1:7443"), "server URL")
	tenantID := fs.String("tenant", "", "tenant configured for OIDC")
	credentialPath := fs.String("credential-file", defaultCredentialPath(), "mode-0600 credential file")
	jsonOutput := fs.Bool("json", false, "JSON output")
	parse(fs, args)
	if err := arity(fs, 0, 0, "login --tenant TENANT"); err != nil {
		return err
	}
	if *tenantID == "" || *credentialPath == "" {
		return errors.New("login requires --tenant and a credential file")
	}
	api := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	configURL := strings.TrimSuffix(*serverURL, "/") + "/v1/identity/oidc?tenant=" + url.QueryEscape(*tenantID)
	var config proto.TenantOIDC
	if err := getLoginJSON(ctx, api, configURL, &config); err != nil {
		return err
	}
	oidc, err := identity.NewOIDCClient(identity.OIDCOptions{Issuer: config.Issuer, ClientID: config.ClientID, Audience: config.Audience, Scopes: config.Scopes})
	if err != nil {
		return err
	}
	discovery, err := oidc.Discover(ctx)
	if err != nil {
		return err
	}
	nonceRaw := make([]byte, 32)
	if _, err := rand.Read(nonceRaw); err != nil {
		return err
	}
	nonce := base64.RawURLEncoding.EncodeToString(nonceRaw)
	device, err := oidc.StartDeviceAuthorizationWithNonce(ctx, discovery, nonce)
	if err != nil {
		return err
	}
	verification := device.VerificationURIComplete
	if verification == "" {
		verification = device.VerificationURI
	}
	fmt.Fprintf(os.Stderr, "Open %s and enter code %s\n", verification, device.UserCode)
	deadline := time.Now().Add(time.Duration(device.ExpiresIn) * time.Second)
	interval := time.Duration(device.Interval) * time.Second
	var provider identity.DeviceTokenResponse
	for {
		if !time.Now().Before(deadline) {
			return identity.ErrExpiredDeviceCode
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		provider, err = oidc.PollDeviceToken(ctx, discovery, device.DeviceCode)
		if err == nil {
			break
		}
		if errors.Is(err, identity.ErrAuthorizationPending) {
			continue
		}
		if errors.Is(err, identity.ErrSlowDown) {
			interval += 5 * time.Second
			continue
		}
		if devicePollBackoff(err) {
			// RFC 8628 §3.5 requires backing off on connection timeouts.
			interval += 5 * time.Second
			continue
		}
		return err
	}
	request := map[string]string{"tenant": *tenantID, "id_token": provider.IDToken, "nonce": nonce}
	var tokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := postLoginJSON(ctx, api, strings.TrimSuffix(*serverURL, "/")+"/v1/identity/oidc/exchange", request, &tokens); err != nil {
		return err
	}
	if tokens.AccessToken == "" || tokens.RefreshToken == "" {
		return errors.New("login returned incomplete credentials")
	}
	if err := writeCredentialFile(*credentialPath, credentialFile{Server: *serverURL, Tenant: *tenantID, AccessToken: tokens.AccessToken, RefreshToken: tokens.RefreshToken, UpdatedAt: time.Now().UnixMilli()}); err != nil {
		return err
	}
	if *jsonOutput {
		printJSON(map[string]any{"ok": true, "tenant": *tenantID, "credential_file": *credentialPath})
	} else {
		fmt.Printf("logged in to %s as tenant %s\n", *serverURL, *tenantID)
	}
	return nil
}

func devicePollBackoff(err error) bool {
	if errors.Is(err, identity.ErrAuthorizationPending) || errors.Is(err, identity.ErrSlowDown) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func getLoginJSON(ctx context.Context, client *http.Client, endpoint string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	return doLoginJSON(client, req, out)
}

func postLoginJSON(ctx context.Context, client *http.Client, endpoint string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return doLoginJSON(client, req, out)
}

func doLoginJSON(client *http.Client, request *http.Request, out any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("login endpoint returned HTTP %d", response.StatusCode)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return errors.New("login endpoint returned invalid JSON")
	}
	return nil
}

func writeCredentialFile(path string, value credentialFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(temporary)
		}
	}()
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	ok = true
	return nil
}
