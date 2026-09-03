package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func (s *Store) sign(req *http.Request, payloadHash string, now time.Time) {
	now = now.UTC()
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if s.session != "" {
		req.Header.Set("X-Amz-Security-Token", s.session)
	}
	if s.accessKey == "" {
		return
	}

	canonicalHeaders, signedHeaders := signedHeaderBlock(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery(req.URL.Query()),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := date + "/" + s.region + "/s3/aws4_request"
	requestSum := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(requestSum[:])
	dateKey := hmacSHA256([]byte("AWS4"+s.secretKey), date)
	regionKey := hmacSHA256(dateKey, s.region)
	serviceKey := hmacSHA256(regionKey, "s3")
	signingKey := hmacSHA256(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func signedHeaderBlock(req *http.Request) (string, string) {
	values := make(map[string][]string)
	values["host"] = []string{req.URL.Host}
	for name, vals := range req.Header {
		lower := strings.ToLower(name)
		// SigV4 requires host and every x-amz-* header. Leaving ordinary HTTP
		// condition and representation headers unsigned matches S3 SDK behavior
		// and avoids incompatible canonicalization by gateways.
		if !strings.HasPrefix(lower, "x-amz-") {
			continue
		}
		values[lower] = append(values[lower], vals...)
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	var block strings.Builder
	for _, name := range names {
		block.WriteString(name)
		block.WriteByte(':')
		for i, value := range values[name] {
			if i > 0 {
				block.WriteByte(',')
			}
			block.WriteString(strings.Join(strings.Fields(value), " "))
		}
		block.WriteByte('\n')
	}
	return block.String(), strings.Join(names, ";")
}

func canonicalURI(u *url.URL) string {
	uri := u.EscapedPath()
	if uri == "" {
		return "/"
	}
	return uri
}

func canonicalQuery(values url.Values) string {
	var parts []string
	for key, valuesForKey := range values {
		vals := append([]string(nil), valuesForKey...)
		if len(vals) == 0 {
			vals = []string{""}
		}
		for _, value := range vals {
			parts = append(parts, awsEscape(key)+"="+awsEscape(value))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func awsEscape(value string) string {
	return awsPercentEncode(value)
}

func awsPercentEncode(value string) string {
	const hexDigits = "0123456789ABCDEF"
	var encoded strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			encoded.WriteByte(c)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte(hexDigits[c>>4])
		encoded.WriteByte(hexDigits[c&15])
	}
	return encoded.String()
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}
