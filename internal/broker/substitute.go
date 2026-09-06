package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"remount.dev/remount/internal/proto"
)

// defaultMaxSubstitutionBodyBytes bounds the request body the broker buffers
// so it can place a credential in a form field or at a JSON pointer. A
// credential-bearing body is a form or a small JSON document, never an
// upload, and this bound is what stops one workspace from sizing the node's
// memory with a body it only needs the broker to read. A rule's
// MaxRequestBytes, when smaller, wins.
const defaultMaxSubstitutionBodyBytes int64 = 1 << 20

// Locations a placeholder was found in. A placeholder is scanned for
// everywhere, whatever its binding declares, because one aimed at a host its
// binding does not cover is an exfiltration attempt wherever it sits.
const (
	foundInHeader = 1 << iota
	foundInQuery
	foundInBody
)

// credentialParts is the part of one request a credential pass may read and,
// in substitute mode, rewrite.
//
// Query is the raw query string; it is empty on a surface that carries none.
// Body is the buffered request body and is nil when the surface did not
// buffer one — an unbuffered body is simply not scanned, exactly like the
// opaque bytes of a CONNECT tunnel, because scanning it would mean buffering
// every upload the workspace makes.
type credentialParts struct {
	Header      http.Header
	Query       string
	Body        []byte
	BodyPresent bool
	ContentType string

	// parsed views, populated lazily and reused by the locate and rewrite
	// passes so one malformed document is diagnosed once.
	queryValues url.Values
	queryErr    error
	queryParsed bool
	formValues  url.Values
	formErr     error
	formParsed  bool
	jsonDoc     any
	jsonErr     error
	jsonParsed  bool
}

func (p *credentialParts) query() (url.Values, error) {
	if !p.queryParsed {
		p.queryParsed = true
		if p.Query == "" {
			p.queryValues = url.Values{}
		} else {
			p.queryValues, p.queryErr = url.ParseQuery(p.Query)
		}
	}
	return p.queryValues, p.queryErr
}

func (p *credentialParts) form() (url.Values, error) {
	if !p.formParsed {
		p.formParsed = true
		p.formValues, p.formErr = url.ParseQuery(string(p.Body))
	}
	return p.formValues, p.formErr
}

func (p *credentialParts) jsonBody() (any, error) {
	if !p.jsonParsed {
		p.jsonParsed = true
		decoder := json.NewDecoder(bytes.NewReader(p.Body))
		// Exact number round-tripping: re-serializing 1e9 as 1000000000
		// changes a body the upstream may have signed.
		decoder.UseNumber()
		if err := decoder.Decode(&p.jsonDoc); err != nil {
			p.jsonErr = err
		} else if decoder.More() {
			p.jsonErr = fmt.Errorf("request body carries more than one JSON document")
		}
	}
	return p.jsonDoc, p.jsonErr
}

// locate reports which parts of the request carry placeholder as a complete
// credential token.
func (p *credentialParts) locate(placeholder string) int {
	found := 0
	for _, values := range p.Header {
		for _, value := range values {
			plain, _ := credentialText(value)
			if containsToken(plain, placeholder) {
				found |= foundInHeader
			}
		}
	}
	if p.Query != "" {
		if containsToken(p.Query, placeholder) {
			found |= foundInQuery
		} else if values, err := p.query(); err == nil {
			for _, list := range values {
				for _, value := range list {
					if containsToken(value, placeholder) {
						found |= foundInQuery
					}
				}
			}
		}
	}
	if p.BodyPresent && len(p.Body) > 0 {
		if containsToken(string(p.Body), placeholder) {
			found |= foundInBody
		} else if isFormContentType(p.ContentType) {
			// A form body percent-encodes ":" so the raw scan above cannot
			// see "ref:b_x"; the decoded values can.
			if values, err := p.form(); err == nil {
				for _, list := range values {
					for _, value := range list {
						if containsToken(value, placeholder) {
							found |= foundInBody
						}
					}
				}
			}
		}
	}
	return found
}

// locationBit maps a declared substitution location to the part of the
// request it lives in.
func locationBit(location string) int {
	switch location {
	case proto.SubstitutionQuery:
		return foundInQuery
	case proto.SubstitutionBodyForm, proto.SubstitutionBodyJSON:
		return foundInBody
	default:
		return foundInHeader
	}
}

// substitutionPlan collects the replacements each part of the request needs.
// Replacements are applied in one non-cascading pass per target so a real
// secret that happens to contain another placeholder is never rewritten a
// second time.
type substitutionPlan struct {
	header []credentialReplacement
	query  map[string][]credentialReplacement
	form   map[string][]credentialReplacement
	json   map[string][]credentialReplacement
}

func (s *substitutionPlan) add(target *map[string][]credentialReplacement, key string, replacement credentialReplacement) {
	if *target == nil {
		*target = map[string][]credentialReplacement{}
	}
	(*target)[key] = append((*target)[key], replacement)
}

func (s *substitutionPlan) empty() bool {
	return len(s.header) == 0 && len(s.query) == 0 && len(s.form) == 0 && len(s.json) == 0
}

// planSubstitution validates that a lease's placeholder sits exactly where
// its binding says and records the replacement. Every ambiguity — a
// parameter that occurs twice, a body the declared location cannot parse, a
// pointer to something other than a string — is a refusal, because guessing
// which occurrence the workspace meant is how a credential ends up somewhere
// the operator never authorized.
func (p *credentialParts) planSubstitution(lease proto.BindingLease, location string, plan *substitutionPlan) *credentialRejection {
	placeholder := Placeholder(lease)
	replacement := credentialReplacement{placeholder: placeholder, secret: lease.Secret}
	deny := func(detail string) *credentialRejection {
		return &credentialRejection{
			decision: DecisionDenied, binding: lease.ID,
			reason: "substitution refused for " + lease.ID + ": " + detail,
			public: "credential " + lease.ID + " cannot be substituted: " + detail,
			status: http.StatusBadRequest, code: proto.CodeBadRequest, denialReason: proto.ReasonEgressDenied,
		}
	}
	switch location {
	case proto.SubstitutionQuery:
		values, err := p.query()
		if err != nil {
			return deny("the request query is malformed")
		}
		list := values[lease.Substitution.Name]
		if len(list) != 1 {
			return deny(fmt.Sprintf("query parameter %q occurs %d times, not once", lease.Substitution.Name, len(list)))
		}
		if !containsToken(list[0], placeholder) {
			return deny("the placeholder is not in query parameter " + strconv.Quote(lease.Substitution.Name))
		}
		plan.add(&plan.query, lease.Substitution.Name, replacement)
	case proto.SubstitutionBodyForm:
		if !p.BodyPresent {
			return deny("the request carries no buffered body")
		}
		if !isFormContentType(p.ContentType) {
			return deny("a form-field substitution needs application/x-www-form-urlencoded")
		}
		values, err := p.form()
		if err != nil {
			return deny("the form body is malformed")
		}
		list := values[lease.Substitution.Name]
		if len(list) != 1 {
			return deny(fmt.Sprintf("form field %q occurs %d times, not once", lease.Substitution.Name, len(list)))
		}
		if !containsToken(list[0], placeholder) {
			return deny("the placeholder is not in form field " + strconv.Quote(lease.Substitution.Name))
		}
		plan.add(&plan.form, lease.Substitution.Name, replacement)
	case proto.SubstitutionBodyJSON:
		if !p.BodyPresent {
			return deny("the request carries no buffered body")
		}
		if !isJSONContentType(p.ContentType) {
			return deny("a JSON-pointer substitution needs application/json")
		}
		doc, err := p.jsonBody()
		if err != nil {
			return deny("the JSON body is malformed")
		}
		current, _, err := jsonPointerString(doc, lease.Substitution.JSONPointer)
		if err != nil {
			return deny(err.Error())
		}
		if !containsToken(current, placeholder) {
			return deny("the placeholder is not at " + strconv.Quote(lease.Substitution.JSONPointer))
		}
		plan.add(&plan.json, lease.Substitution.JSONPointer, replacement)
	default:
		plan.header = append(plan.header, replacement)
	}
	return nil
}

// apply rewrites the request in place. It runs only after every lease has
// been validated, so a refusal never leaves a half-substituted request.
func (p *credentialParts) apply(plan *substitutionPlan) *credentialRejection {
	fail := func(detail string) *credentialRejection {
		return &credentialRejection{
			decision: DecisionDenied, reason: "substitution failed: " + detail,
			public: "credential substitution failed: " + detail,
			status: http.StatusBadRequest, code: proto.CodeBadRequest, denialReason: proto.ReasonEgressDenied,
		}
	}
	if len(plan.header) > 0 {
		for name, values := range p.Header {
			for i, value := range values {
				plain, basic := credentialText(value)
				// Rebuild only the values that carry a placeholder. Every
				// header of every credentialed request passes through here.
				if !carriesPlaceholder(plain, plan.header) {
					continue
				}
				rewritten := substituteAll(plain, plan.header)
				if basic {
					rewritten = "Basic " + encodeBasic(rewritten)
				}
				values[i] = rewritten
			}
			p.Header[name] = values
		}
	}
	if len(plan.query) > 0 {
		values, err := p.query()
		if err != nil {
			return fail("the request query is malformed")
		}
		for name, replacements := range plan.query {
			values[name] = []string{substituteAll(values[name][0], replacements)}
		}
		p.Query = values.Encode()
	}
	if len(plan.form) > 0 {
		values, err := p.form()
		if err != nil {
			return fail("the form body is malformed")
		}
		for name, replacements := range plan.form {
			values[name] = []string{substituteAll(values[name][0], replacements)}
		}
		p.Body = []byte(values.Encode())
	}
	if len(plan.json) > 0 {
		doc, err := p.jsonBody()
		if err != nil {
			return fail("the JSON body is malformed")
		}
		for pointer, replacements := range plan.json {
			current, set, err := jsonPointerString(doc, pointer)
			if err != nil {
				return fail(err.Error())
			}
			set(substituteAll(current, replacements))
		}
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		// A credential is not HTML and the upstream is not a browser;
		// escaping < > & would change bytes the workspace wrote.
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(doc); err != nil {
			return fail("the substituted JSON body cannot be serialized")
		}
		p.Body = bytes.TrimRight(buffer.Bytes(), "\n")
	}
	return nil
}

// carriesPlaceholder reports whether value holds any of these replacements'
// placeholders as a complete credential token.
func carriesPlaceholder(value string, replacements []credentialReplacement) bool {
	for _, replacement := range replacements {
		if containsToken(value, replacement.placeholder) {
			return true
		}
	}
	return false
}

func isFormContentType(contentType string) bool {
	media, _, err := mime.ParseMediaType(contentType)
	return err == nil && strings.EqualFold(media, "application/x-www-form-urlencoded")
}

func isJSONContentType(contentType string) bool {
	media, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	media = strings.ToLower(media)
	return media == "application/json" || media == "text/json" || strings.HasSuffix(media, "+json")
}

// jsonPointerString resolves an RFC 6901 pointer to a JSON string and returns
// a setter that replaces it in place. A pointer that names nothing, traverses
// a scalar, or lands on anything other than a string is an error: the broker
// refuses rather than inventing a location for a credential.
func jsonPointerString(doc any, pointer string) (string, func(string), error) {
	if !strings.HasPrefix(pointer, "/") {
		return "", nil, fmt.Errorf("json pointer %s is not absolute", strconv.Quote(pointer))
	}
	tokens := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	current := doc
	for i, raw := range tokens {
		token := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		last := i == len(tokens)-1
		switch container := current.(type) {
		case map[string]any:
			next, ok := container[token]
			if !ok {
				return "", nil, fmt.Errorf("json pointer %s names no member", strconv.Quote(pointer))
			}
			if last {
				value, ok := next.(string)
				if !ok {
					return "", nil, fmt.Errorf("json pointer %s does not name a string", strconv.Quote(pointer))
				}
				return value, func(v string) { container[token] = v }, nil
			}
			current = next
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(container) {
				return "", nil, fmt.Errorf("json pointer %s names no element", strconv.Quote(pointer))
			}
			if last {
				value, ok := container[index].(string)
				if !ok {
					return "", nil, fmt.Errorf("json pointer %s does not name a string", strconv.Quote(pointer))
				}
				return value, func(v string) { container[index] = v }, nil
			}
			current = container[index]
		default:
			return "", nil, fmt.Errorf("json pointer %s traverses a value that is not a container", strconv.Quote(pointer))
		}
	}
	return "", nil, fmt.Errorf("json pointer %s is empty", strconv.Quote(pointer))
}
