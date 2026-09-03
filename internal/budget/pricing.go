package budget

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxPrices               = 4096
	maxTenantOverrides      = 4096
	maxTenantOverridePrices = 64 << 10
	maxPricingBytes         = 4 << 20
	maxPriceMicrosPerMTok   = int64(1_000_000_000_000)
	tokensPerMillion        = int64(1_000_000)
)

//go:embed pricing.json
var defaultPricingJSON []byte

// Price is an approximate text-token price in micro-USD per million tokens.
type Price struct {
	Provider                     string `json:"provider"`
	Model                        string `json:"model"`
	InputMicrosPerMillionTokens  int64  `json:"input_micros_per_million_tokens"`
	OutputMicrosPerMillionTokens int64  `json:"output_micros_per_million_tokens"`
	Approximate                  bool   `json:"approximate"`
	Source                       string `json:"source,omitempty"`
}

type priceDocument struct {
	Version  int     `json:"version"`
	AsOf     string  `json:"as_of"`
	Currency string  `json:"currency"`
	Prices   []Price `json:"prices"`
}

// Catalog contains immutable defaults and bounded tenant-specific overrides.
type Catalog struct {
	mu             sync.RWMutex
	defaults       map[string]Price
	overrides      map[string]map[string]Price
	overridePrices int
}

// DefaultCatalog loads the checked-in approximate pricing catalogue.
func DefaultCatalog() (*Catalog, error) { return ParseCatalog(defaultPricingJSON) }

// ParseCatalog strictly parses a pricing document. It is exported for operator
// supplied catalogues and deterministic tests.
func ParseCatalog(data []byte) (*Catalog, error) {
	if len(data) == 0 || len(data) > maxPricingBytes {
		return nil, errors.New("budget: invalid pricing catalogue")
	}
	var document priceDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil || document.Version != 1 || document.Currency != "USD" || len(document.Prices) == 0 || len(document.Prices) > maxPrices {
		return nil, errors.New("budget: invalid pricing catalogue")
	}
	if _, err := time.Parse("2006-01-02", document.AsOf); err != nil {
		return nil, errors.New("budget: invalid pricing catalogue")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("budget: invalid pricing catalogue")
	}
	prices, err := indexPrices(document.Prices)
	if err != nil {
		return nil, err
	}
	return &Catalog{defaults: prices, overrides: make(map[string]map[string]Price)}, nil
}

// ReplaceTenant atomically replaces one tenant's bounded price overrides.
// An empty list removes the override set.
func (c *Catalog) ReplaceTenant(tenant string, prices []Price) error {
	if !safeID(tenant) || len(prices) > maxPrices {
		return errors.New("budget: invalid tenant price override")
	}
	indexed, err := indexPrices(prices)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(indexed) == 0 {
		c.overridePrices -= len(c.overrides[tenant])
		delete(c.overrides, tenant)
		return nil
	}
	if c.overrides[tenant] == nil && len(c.overrides) >= maxTenantOverrides {
		return ErrCapacity
	}
	projected := c.overridePrices - len(c.overrides[tenant]) + len(indexed)
	if projected > maxTenantOverridePrices {
		return ErrCapacity
	}
	c.overrides[tenant] = indexed
	c.overridePrices = projected
	return nil
}

// Lookup returns the tenant override when present, then the checked-in price.
func (c *Catalog) Lookup(tenant, provider, model string) (Price, bool) {
	key := priceKey(provider, model)
	c.mu.RLock()
	defer c.mu.RUnlock()
	if prices := c.overrides[tenant]; prices != nil {
		if price, ok := prices[key]; ok {
			return price, true
		}
	}
	price, ok := c.defaults[key]
	return price, ok
}

// Estimate returns a rounded-up approximate cost in micro-USD.
func (c *Catalog) Estimate(tenant, provider, model string, inputTokens, outputTokens int64) (int64, bool) {
	price, ok := c.Lookup(tenant, provider, model)
	if !ok || inputTokens < 0 || outputTokens < 0 {
		return 0, false
	}
	input, ok := scaledCost(inputTokens, price.InputMicrosPerMillionTokens)
	if !ok {
		return 0, false
	}
	output, ok := scaledCost(outputTokens, price.OutputMicrosPerMillionTokens)
	if !ok || input > maxInt64-output {
		return 0, false
	}
	return input + output, true
}

// Prices returns a stable value copy without tenant overrides.
func (c *Catalog) Prices() []Price {
	c.mu.RLock()
	defer c.mu.RUnlock()
	prices := make([]Price, 0, len(c.defaults))
	for _, price := range c.defaults {
		prices = append(prices, price)
	}
	sort.Slice(prices, func(i, j int) bool {
		if prices[i].Provider == prices[j].Provider {
			return prices[i].Model < prices[j].Model
		}
		return prices[i].Provider < prices[j].Provider
	})
	return prices
}

func indexPrices(prices []Price) (map[string]Price, error) {
	indexed := make(map[string]Price, len(prices))
	for _, price := range prices {
		if !safeID(price.Provider) || !safeID(price.Model) || len(price.Source) > 1024 || price.InputMicrosPerMillionTokens < 0 || price.OutputMicrosPerMillionTokens < 0 || price.InputMicrosPerMillionTokens > maxPriceMicrosPerMTok || price.OutputMicrosPerMillionTokens > maxPriceMicrosPerMTok || !price.Approximate {
			return nil, errors.New("budget: invalid approximate price")
		}
		key := priceKey(price.Provider, price.Model)
		if _, exists := indexed[key]; exists {
			return nil, errors.New("budget: duplicate price")
		}
		indexed[key] = price
	}
	return indexed, nil
}

func priceKey(provider, model string) string {
	return strings.ToLower(provider) + "\x00" + strings.ToLower(model)
}

func scaledCost(tokens, rate int64) (int64, bool) {
	if tokens < 0 || tokens > MaxTokenCount || rate < 0 || rate > maxPriceMicrosPerMTok {
		return 0, false
	}
	whole := tokens / tokensPerMillion
	remainder := tokens % tokensPerMillion
	if whole != 0 && rate > maxInt64/whole {
		return 0, false
	}
	cost := whole * rate
	partial := remainder * rate
	if partial > 0 {
		partial = (partial + tokensPerMillion - 1) / tokensPerMillion
	}
	if cost > maxInt64-partial {
		return 0, false
	}
	return cost + partial, true
}
