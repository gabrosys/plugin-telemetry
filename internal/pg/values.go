package pg

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// ResultFormats asks for binary results of the types pgx's database/sql driver asks for.
// Ported from pgx v5.13.0 stdlib/sql.go:120-135 (MIT).
var ResultFormats = pgx.QueryResultFormatsByOID{
	pgtype.BoolOID:        1,
	pgtype.ByteaOID:       1,
	pgtype.CIDOID:         1,
	pgtype.DateOID:        1,
	pgtype.Float4OID:      1,
	pgtype.Float8OID:      1,
	pgtype.Int2OID:        1,
	pgtype.Int4OID:        1,
	pgtype.Int8OID:        1,
	pgtype.OIDOID:         1,
	pgtype.TimestampOID:   1,
	pgtype.TimestamptzOID: 1,
	pgtype.XIDOID:         1,
}

// Values converts one row of raw column values to the Go types pgx's database/sql driver
// returns: int64, float64, bool, []byte, string, time.Time or nil.
// Ported from pgx v5.13.0 stdlib/sql.go:783-911 (MIT).
func Values(m *pgtype.Map, fields []pgconn.FieldDescription, raw [][]byte) ([]any, error) {
	out := make([]any, len(raw))
	for i, src := range raw {
		if src == nil {
			continue
		}
		v, err := value(m, fields[i], src)
		if err != nil {
			return nil, fmt.Errorf("convert field %d: %w", i, err)
		}
		out[i] = v
	}
	return out, nil
}

func value(m *pgtype.Map, fd pgconn.FieldDescription, src []byte) (any, error) {
	switch fd.DataTypeOID {
	case pgtype.BoolOID:
		var d bool
		err := scan(m, fd, src, &d)
		return d, err
	case pgtype.ByteaOID, pgtype.JSONOID, pgtype.JSONBOID, pgtype.XMLOID:
		var d []byte
		if err := scan(m, fd, src, &d); err != nil {
			return nil, err
		}
		return d, nil
	case pgtype.CIDOID, pgtype.OIDOID, pgtype.XIDOID:
		return scanValue[pgtype.Uint32](m, fd, src)
	case pgtype.DateOID:
		return scanValue[pgtype.Date](m, fd, src)
	case pgtype.Float4OID:
		var d float32
		err := scan(m, fd, src, &d)
		return float64(d), err
	case pgtype.Float8OID:
		var d float64
		err := scan(m, fd, src, &d)
		return d, err
	case pgtype.Int2OID:
		var d int16
		err := scan(m, fd, src, &d)
		return int64(d), err
	case pgtype.Int4OID:
		var d int32
		err := scan(m, fd, src, &d)
		return int64(d), err
	case pgtype.Int8OID:
		var d int64
		err := scan(m, fd, src, &d)
		return d, err
	case pgtype.TimestampOID:
		return scanValue[pgtype.Timestamp](m, fd, src)
	case pgtype.TimestamptzOID:
		return scanValue[pgtype.Timestamptz](m, fd, src)
	default:
		var d string
		err := scan(m, fd, src, &d)
		return d, err
	}
}

func scan(m *pgtype.Map, fd pgconn.FieldDescription, src []byte, dst any) error {
	if err := m.PlanScan(fd.DataTypeOID, fd.Format, dst).Scan(src, dst); err != nil {
		return fmt.Errorf("scan type %d: %w", fd.DataTypeOID, err)
	}
	return nil
}

// scanValue scans src into a T and returns the driver value of the result.
func scanValue[T any, P interface {
	*T
	driver.Valuer
}](m *pgtype.Map, fd pgconn.FieldDescription, src []byte) (any, error) {
	var d T
	p := P(&d)
	if err := scan(m, fd, src, p); err != nil {
		return nil, err
	}
	v, err := p.Value()
	if err != nil {
		return nil, fmt.Errorf("value of type %d: %w", fd.DataTypeOID, err)
	}
	return v, nil
}
