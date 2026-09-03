package encrypted

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// MultiMasterKey writes with Current and dispatches old ciphertext by key id.
// It permits master-key rotation while tenant-key records are rewrapped.
type MultiMasterKey struct {
	Current MasterKey
	ByID    map[string]MasterKey
}

// Wrap uses the current master key.
func (m MultiMasterKey) Wrap(ctx context.Context, plaintext, aad []byte) (WrappedKey, error) {
	if m.Current == nil {
		return WrappedKey{}, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	return m.Current.Wrap(ctx, plaintext, aad)
}

// Unwrap selects the provider recorded with the wrapped tenant key.
func (m MultiMasterKey) Unwrap(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error) {
	provider := m.ByID[wrapped.KeyID]
	if provider == nil {
		return nil, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	return provider.Unwrap(ctx, wrapped, aad)
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// AWSKMSMasterKey uses the AWS KMS Encrypt and Decrypt JSON API. Client must
// authenticate requests, normally with a SigV4 RoundTripper supplied by the
// control plane; no cloud SDK is required or imported here.
type AWSKMSMasterKey struct {
	Endpoint string
	KeyID    string
	Client   httpDoer
}

// Wrap asks AWS KMS to encrypt one tenant key with authenticated context.
func (k *AWSKMSMasterKey) Wrap(ctx context.Context, plaintext, aad []byte) (WrappedKey, error) {
	var response struct {
		Ciphertext string `json:"CiphertextBlob"`
		KeyID      string `json:"KeyId"`
	}
	err := k.call(ctx, "TrentService.Encrypt", map[string]any{
		"KeyId": k.KeyID, "Plaintext": base64.StdEncoding.EncodeToString(plaintext),
		"EncryptionContext": map[string]string{"remount_aad": base64.RawStdEncoding.EncodeToString(aad)},
	}, &response)
	if err != nil {
		return WrappedKey{}, err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(response.Ciphertext)
	if err != nil || len(ciphertext) == 0 || len(ciphertext) > 1<<20 || response.KeyID == "" || len(response.KeyID) > 1024 {
		clear(ciphertext)
		return WrappedKey{}, fmt.Errorf("%w", ErrMalformed)
	}
	return WrappedKey{KeyID: response.KeyID, Ciphertext: ciphertext}, nil
}

// Unwrap asks AWS KMS to decrypt one tenant key with the same context.
func (k *AWSKMSMasterKey) Unwrap(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error) {
	if wrapped.KeyID == "" || len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > 1<<20 {
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	var response struct {
		Plaintext string `json:"Plaintext"`
	}
	err := k.call(ctx, "TrentService.Decrypt", map[string]any{
		"KeyId": wrapped.KeyID, "CiphertextBlob": base64.StdEncoding.EncodeToString(wrapped.Ciphertext),
		"EncryptionContext": map[string]string{"remount_aad": base64.RawStdEncoding.EncodeToString(aad)},
	}, &response)
	if err != nil {
		return nil, err
	}
	plaintext, err := base64.StdEncoding.DecodeString(response.Plaintext)
	if err != nil || len(plaintext) == 0 || len(plaintext) > 4096 {
		clear(plaintext)
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	return plaintext, nil
}

func (k *AWSKMSMasterKey) call(ctx context.Context, target string, input any, output any) error {
	if k.Client == nil || k.Endpoint == "" || k.KeyID == "" {
		return errors.New("encrypted artifact: AWS KMS provider is incomplete")
	}
	return callJSON(ctx, k.Client, k.Endpoint, map[string]string{
		"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": target,
	}, input, output, "AWS KMS")
}

// GCPKMSMasterKey uses the Cloud KMS REST encrypt/decrypt API. Client must add
// an OAuth bearer credential through its Transport.
type GCPKMSMasterKey struct {
	// Resource is projects/.../locations/.../keyRings/.../cryptoKeys/....
	Resource string
	Endpoint string
	Client   httpDoer
}

// Wrap asks Cloud KMS to encrypt one tenant key.
func (k *GCPKMSMasterKey) Wrap(ctx context.Context, plaintext, aad []byte) (WrappedKey, error) {
	var response struct {
		Ciphertext string `json:"ciphertext"`
		Name       string `json:"name"`
	}
	err := k.call(ctx, ":encrypt", map[string]string{
		"plaintext":                   base64.StdEncoding.EncodeToString(plaintext),
		"additionalAuthenticatedData": base64.StdEncoding.EncodeToString(aad),
	}, &response)
	if err != nil {
		return WrappedKey{}, err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(response.Ciphertext)
	if err != nil || len(ciphertext) == 0 || len(ciphertext) > 1<<20 {
		clear(ciphertext)
		return WrappedKey{}, fmt.Errorf("%w", ErrMalformed)
	}
	keyID := response.Name
	if keyID == "" {
		keyID = k.Resource
	}
	if len(keyID) > 2048 {
		clear(ciphertext)
		return WrappedKey{}, fmt.Errorf("%w", ErrMalformed)
	}
	return WrappedKey{KeyID: keyID, Ciphertext: ciphertext}, nil
}

// Unwrap asks Cloud KMS to decrypt one tenant key.
func (k *GCPKMSMasterKey) Unwrap(ctx context.Context, wrapped WrappedKey, aad []byte) ([]byte, error) {
	if wrapped.KeyID == "" || len(wrapped.Ciphertext) == 0 || len(wrapped.Ciphertext) > 1<<20 {
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	var response struct {
		Plaintext string `json:"plaintext"`
	}
	err := k.call(ctx, ":decrypt", map[string]string{
		"ciphertext":                  base64.StdEncoding.EncodeToString(wrapped.Ciphertext),
		"additionalAuthenticatedData": base64.StdEncoding.EncodeToString(aad),
	}, &response)
	if err != nil {
		return nil, err
	}
	plaintext, err := base64.StdEncoding.DecodeString(response.Plaintext)
	if err != nil || len(plaintext) == 0 || len(plaintext) > 4096 {
		clear(plaintext)
		return nil, fmt.Errorf("%w", ErrMalformed)
	}
	return plaintext, nil
}

func (k *GCPKMSMasterKey) call(ctx context.Context, operation string, input any, output any) error {
	if k.Client == nil || k.Endpoint == "" || k.Resource == "" || strings.Contains(k.Resource, "..") {
		return errors.New("encrypted artifact: GCP KMS provider is incomplete")
	}
	url := strings.TrimRight(k.Endpoint, "/") + "/v1/" + strings.TrimLeft(k.Resource, "/") + operation
	return callJSON(ctx, k.Client, url, map[string]string{"Content-Type": "application/json"}, input, output, "GCP KMS")
}

func callJSON(ctx context.Context, client httpDoer, url string, headers map[string]string, input, output any, provider string) error {
	body, err := json.Marshal(input)
	if err != nil {
		return errors.New("encrypted artifact: KMS request encoding failed")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		clear(body)
		return errors.New("encrypted artifact: KMS request construction failed")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := client.Do(req)
	clear(body)
	if err != nil {
		return fmt.Errorf("encrypted artifact: %s request failed", provider)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("encrypted artifact: %s returned HTTP %d", provider, resp.StatusCode)
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(responseBody) > 1<<20 {
		clear(responseBody)
		return fmt.Errorf("encrypted artifact: %s returned an oversized response", provider)
	}
	defer clear(responseBody)
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("encrypted artifact: %s returned malformed JSON", provider)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("encrypted artifact: %s returned trailing JSON", provider)
	}
	return nil
}
