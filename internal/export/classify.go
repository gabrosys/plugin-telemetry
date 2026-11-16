package export

import (
	"context"
	"errors"
	"math"
	"regexp"
	"strconv"

	"google.golang.org/grpc/status"
)

// Values of semconv error.type set by Classify, besides HTTP statuses and gRPC code names.
const (
	errorTypeRejected = "rejected"
	errorTypeTimeout  = "timeout"
	errorTypeTooLarge = "request_too_large"
	errorTypeOther    = "_OTHER"
)

var (
	// The partial-success error type is internal to the exporters, so its message is matched:
	// "OTLP partial success: <msg> (<n> metric data points rejected)"
	// (otlpmetrichttp and otlpmetricgrpc v1.47.0 internal/partialsuccess.go:24-30,59-66).
	partialSuccess = regexp.MustCompile(`^OTLP partial success: (?s:.*) \((\d+) metric data points rejected\)$`)
	// The size cap returns an unexported error: otlpmetrichttp v1.47.0 client.go:155 and
	// otlpmetricgrpc v1.47.0 client.go:126.
	tooLarge = regexp.MustCompile(`^request (?:body|message) too large: exceeded \d+ bytes$`)
)

// httpStatusError is an export whose last attempt got a non-2xx HTTP response.
type httpStatusError struct {
	code int
	err  error
}

func (e *httpStatusError) Error() string { return e.err.Error() }

func (e *httpStatusError) Unwrap() error { return e.err }

// Classify returns semconv error.type and the rejected point count of an export error.
// Partial success is "rejected"; its count is parsed from "(<n> metric data points rejected)".
func Classify(err error) (errorType string, rejected int) {
	if err == nil {
		return "", 0
	}
	if n, ok := rejectedPoints(err); ok {
		return errorTypeRejected, n
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorTypeTimeout, 0
	}
	if anyInTree(err, func(e error) bool { return tooLarge.MatchString(e.Error()) }) {
		return errorTypeTooLarge, 0
	}
	if se, ok := errors.AsType[*httpStatusError](err); ok {
		return strconv.Itoa(se.code), 0
	}
	if s, ok := status.FromError(err); ok {
		return s.Code().String(), 0
	}
	return errorTypeOther, 0
}

// rejectedPoints returns the data points a partial-success error says the backend rejected.
func rejectedPoints(err error) (int, bool) {
	n, found := 0, false
	anyInTree(err, func(e error) bool {
		m := partialSuccess.FindStringSubmatch(e.Error())
		if m == nil {
			return false
		}
		v, perr := strconv.Atoi(m[1])
		if perr != nil {
			v = math.MaxInt // only digits match, so the error is an overflow
		}
		n, found = v, true
		return true
	})
	return n, found
}

// anyInTree reports whether match holds for err or any error it wraps.
func anyInTree(err error, match func(error) bool) bool {
	if err == nil {
		return false
	}
	if match(err) {
		return true
	}
	switch u := err.(type) { //nolint:errorlint // walks the wrap tree itself
	case interface{ Unwrap() []error }:
		for _, e := range u.Unwrap() {
			if anyInTree(e, match) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return anyInTree(u.Unwrap(), match)
	}
	return false
}
