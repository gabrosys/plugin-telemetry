//go:build integration

package pg_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gabrosys/plugin-telemetry/internal/pg"
)

const allTypesSQL = `SELECT 1::int2, 2::int4, 3::int8, 1.5::float4, 2.5::float8, 12.34::numeric,
	'txt'::text, 'vc'::varchar, 'nm'::name, true::bool, '2024-01-02'::date,
	'2024-01-02 03:04:05.678'::timestamp, '2024-01-02 03:04:05+02'::timestamptz,
	'1 day 02:03:04'::interval, 42::oid, '7'::xid, '{"a":1}'::json, '{"a": 1}'::jsonb,
	'\xdeadbeef'::bytea, ARRAY[1.5,2.5]::float8[], ARRAY[1,2]::int8[], '10.0.0.1/32'::inet, NULL`

const manyRowsSQL = `SELECT i::int4, decode(lpad(to_hex(i), 2, '0'), 'hex'), jsonb_build_object('i', i),
	CASE WHEN i = 2 THEN NULL ELSE i::text END
	FROM generate_series(1, 4) AS g(i) ORDER BY g.i`

var _ = Describe("Values", func() {
	var (
		conn *pgx.Conn
		db   *sql.DB
	)

	BeforeEach(func(ctx SpecContext) {
		cfg := server.MonitorConfig(GinkgoT().TempDir())
		cc, err := pg.ConnConfig(cfg, "app")
		Expect(err).NotTo(HaveOccurred())

		conn, err = pgx.ConnectConfig(ctx, cc)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			Expect(conn.Close(ctx)).To(Succeed())
		})

		db = stdlib.OpenDB(*cc)
		DeferCleanup(func() {
			Expect(db.Close()).To(Succeed())
		})
	})

	viaStdlib := func(ctx context.Context, query string) [][]any {
		GinkgoHelper()
		rows, err := db.QueryContext(ctx, query)
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(rows.Close()).To(Succeed()) }()
		cols, err := rows.Columns()
		Expect(err).NotTo(HaveOccurred())

		var out [][]any
		for rows.Next() {
			row := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range row {
				ptrs[i] = &row[i]
			}
			Expect(rows.Scan(ptrs...)).To(Succeed())
			out = append(out, row)
		}
		Expect(rows.Err()).To(Succeed())
		return out
	}

	viaValues := func(ctx context.Context, query string) [][]any {
		GinkgoHelper()
		rows, err := conn.Query(ctx, query, pg.ResultFormats)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		var out [][]any
		for rows.Next() {
			row, err := pg.Values(conn.TypeMap(), rows.FieldDescriptions(), rows.RawValues())
			Expect(err).NotTo(HaveOccurred())
			out = append(out, row)
		}
		Expect(rows.Err()).To(Succeed())
		return out
	}

	It("gives each type the value pgx's database/sql driver gives it", func(ctx SpecContext) {
		want := viaStdlib(ctx, allTypesSQL)
		got := viaValues(ctx, allTypesSQL)

		Expect(want).To(HaveLen(1))
		Expect(got).To(Equal(want))

		Expect(got[0][0]).To(Equal(int64(1)))
		Expect(got[0][3]).To(Equal(float64(1.5)))
		Expect(got[0][9]).To(BeTrue())
		Expect(got[0][10]).To(BeAssignableToTypeOf(time.Time{}))
		Expect(got[0][18]).To(Equal([]byte{0xde, 0xad, 0xbe, 0xef}))
		Expect(got[0][22]).To(BeNil())
	})

	It("does not alias buffers between rows", func(ctx SpecContext) {
		want := viaStdlib(ctx, manyRowsSQL)
		got := viaValues(ctx, manyRowsSQL)

		Expect(want).To(HaveLen(4))
		Expect(got).To(Equal(want))
	})
})
