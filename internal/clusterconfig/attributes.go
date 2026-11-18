package clusterconfig

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

// ErrInvalidResourceAttributes is returned for an OTEL_RESOURCE_ATTRIBUTES value that does not parse.
var ErrInvalidResourceAttributes = errors.New("invalid OTEL_RESOURCE_ATTRIBUTES")

// ParseResourceAttributes parses OTEL_RESOURCE_ATTRIBUTES per the OpenTelemetry specification v1.62.0,
// resource/sdk.md: "k=v,k2=v2", keys and values percent-decoded, the whole value rejected on any error.
// Errors give the entry's position, never its content.
func ParseResourceAttributes(s string) (map[string]string, error) {
	out := map[string]string{}
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}

	for i, pair := range strings.Split(s, ",") {
		rawKey, rawValue, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("%w: entry %d has no '='", ErrInvalidResourceAttributes, i+1)
		}
		rawKey = strings.TrimSpace(rawKey)
		if rawKey == "" {
			return nil, fmt.Errorf("%w: entry %d has an empty key", ErrInvalidResourceAttributes, i+1)
		}

		if strings.Contains(rawValue, "=") {
			return nil, fmt.Errorf("%w: entry %d has an unencoded '=' in its value", ErrInvalidResourceAttributes, i+1)
		}

		key, err := url.PathUnescape(rawKey)
		if err != nil || !utf8.ValidString(key) {
			return nil, fmt.Errorf("%w: entry %d has a bad percent-encoding or invalid UTF-8 in its key", ErrInvalidResourceAttributes, i+1)
		}
		value, err := url.PathUnescape(strings.TrimSpace(rawValue))
		if err != nil || !utf8.ValidString(value) {
			return nil, fmt.Errorf("%w: entry %d has a bad percent-encoding or invalid UTF-8 in its value", ErrInvalidResourceAttributes, i+1)
		}
		out[key] = value
	}

	return out, nil
}
