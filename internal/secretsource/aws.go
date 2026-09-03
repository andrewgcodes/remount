package secretsource

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type awsSource struct {
	arn       string
	region    string
	dnsSuffix string
}

func parseAWSSource(source string) (awsSource, error) {
	arn := strings.TrimPrefix(source, "awssm://")
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) != 7 || parts[0] != "arn" || parts[2] != "secretsmanager" || !awsRegion(parts[3]) || !awsAccount(parts[4]) || parts[5] != "secret" || parts[6] == "" || strings.ContainsAny(parts[6], "?#\r\n") {
		return awsSource{}, errors.New("secretsource: malformed AWS Secrets Manager source")
	}
	var dnsSuffix string
	switch parts[1] {
	case "aws", "aws-us-gov":
		dnsSuffix = "amazonaws.com"
	case "aws-cn":
		dnsSuffix = "amazonaws.com.cn"
	default:
		return awsSource{}, errors.New("secretsource: unsupported AWS partition")
	}
	return awsSource{arn: arn, region: parts[3], dnsSuffix: dnsSuffix}, nil
}

func awsRegion(region string) bool {
	if region == "" {
		return false
	}
	for i := range len(region) {
		c := region[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		return false
	}
	return true
}

func awsAccount(account string) bool {
	if len(account) != 12 {
		return false
	}
	for i := range len(account) {
		if account[i] < '0' || account[i] > '9' {
			return false
		}
	}
	return true
}

func (r *CachedResolver) resolveAWS(ctx context.Context, source string) ([]byte, time.Time, error) {
	parsed, err := parseAWSSource(source)
	if err != nil {
		return nil, time.Time{}, err
	}
	credentials, err := r.awsCredentials(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
		return nil, time.Time{}, errors.New("secretsource: AWS credentials unavailable")
	}
	payload, _ := json.Marshal(struct {
		SecretID string `json:"SecretId"`
	}{SecretID: parsed.arn})
	defer zero(payload)
	endpoint := r.cfg.AWSEndpoint
	if endpoint == "" {
		endpoint = "https://secretsmanager." + parsed.region + "." + parsed.dnsSuffix + "/"
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, time.Time{}, errors.New("secretsource: AWS endpoint is invalid")
	}
	if u.Scheme != "https" && !r.cfg.AllowInsecureHTTP {
		return nil, time.Time{}, errors.New("secretsource: plaintext AWS transport is disabled")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, time.Time{}, errors.New("secretsource: AWS request is invalid")
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "secretsmanager.GetSecretValue")
	signAWSRequest(request, payload, parsed.region, credentials, r.cfg.Now())
	response, err := r.send(request)
	if err != nil {
		return nil, time.Time{}, err
	}
	var result struct {
		SecretString *string `json:"SecretString"`
		SecretBinary string  `json:"SecretBinary"`
	}
	if err := decodeJSON(response, r.cfg.MaxBytes, &result); err != nil {
		return nil, time.Time{}, err
	}
	if result.SecretString != nil && *result.SecretString != "" {
		return []byte(*result.SecretString), time.Time{}, nil
	}
	if result.SecretBinary != "" {
		value, err := base64.StdEncoding.DecodeString(result.SecretBinary)
		if err != nil || len(value) == 0 {
			return nil, time.Time{}, errors.New("secretsource: AWS response contained invalid secret binary")
		}
		return value, time.Time{}, nil
	}
	return nil, time.Time{}, errors.New("secretsource: AWS response omitted secret value")
}

func (r *CachedResolver) awsCredentials(ctx context.Context) (AWSCredentials, error) {
	if r.cfg.AWSCredentials != nil {
		credentials, err := r.cfg.AWSCredentials(ctx)
		if err != nil {
			return AWSCredentials{}, errors.New("secretsource: AWS credentials unavailable")
		}
		return credentials, nil
	}
	return AWSCredentials{
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
	}, nil
}

func signAWSRequest(request *http.Request, payload []byte, region string, credentials AWSCredentials, now time.Time) {
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	payloadSum := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(payloadSum[:])
	request.Header.Set("X-Amz-Date", amzDate)
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if credentials.SessionToken != "" {
		request.Header.Set("X-Amz-Security-Token", credentials.SessionToken)
	}
	headerNames := []string{"content-type", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-target"}
	if credentials.SessionToken != "" {
		headerNames = append(headerNames, "x-amz-security-token")
	}
	sort.Strings(headerNames)
	var canonicalHeaders strings.Builder
	for _, name := range headerNames {
		value := request.Header.Get(name)
		if name == "host" {
			value = request.URL.Host
		}
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.Join(strings.Fields(value), " "))
		canonicalHeaders.WriteByte('\n')
	}
	uri := request.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	signedHeaders := strings.Join(headerNames, ";")
	canonicalRequest := request.Method + "\n" + uri + "\n" + request.URL.RawQuery + "\n" + canonicalHeaders.String() + "\n" + signedHeaders + "\n" + payloadHash
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	scope := date + "/" + region + "/secretsmanager/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(requestHash[:])
	dateKey := awsHMAC([]byte("AWS4"+credentials.SecretAccessKey), date)
	regionKey := awsHMAC(dateKey, region)
	serviceKey := awsHMAC(regionKey, "secretsmanager")
	signingKey := awsHMAC(serviceKey, "aws4_request")
	signature := hex.EncodeToString(awsHMAC(signingKey, stringToSign))
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+credentials.AccessKeyID+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func awsHMAC(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write([]byte(value))
	return hash.Sum(nil)
}
