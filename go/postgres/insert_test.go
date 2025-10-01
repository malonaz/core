package postgres

import (
	"reflect"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

const sqlInsertQueryExample = `
INSERT INTO table_name %s
VALUES %s
ON CONFLICT (column_name) DO NOTHING
`

type Sample struct {
	B int    `db:"ya"`
	a int    `db:"yo"`
	C string `db:"bla"`
	D []string
	e int           `dbbb:"notthisone"`
	F pq.Int64Array `db:"pqarray"`
}

type Wrapper struct {
	Field string `db:"field"`
	Sample
}

var (
	sample1 = &Sample{
		B: 1,
		a: 2,
		C: "yo",
		D: []string{"adfadf", "sdfd"},
		e: 4,
		F: pq.Int64Array{1, 2, 3},
	}

	sample2 = &Sample{
		B: 2,
		a: 3,
		C: "ya",
		D: []string{"kjhkjh"},
		e: 5,
		F: pq.Int64Array{4, 5, 6},
	}

	singleSample = []*Sample{sample1}
	twoSamples   = []*Sample{sample1, sample2}
)

func TestGetParams(t *testing.T) {
	t.Run("SingleObjectNoColumns", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleSample), []string{})
		require.Equal(t, []string{"ya", "bla", "pqarray"}, tags)
		require.Equal(t, []any{sample1.B, sample1.C, sample1.F}, params)
	})

	t.Run("TwoObjectNoColumns", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(twoSamples), []string{})
		require.Equal(t, []string{"ya", "bla", "pqarray"}, tags)
		expectedParams := []any{
			sample1.B, sample1.C, sample1.F,
			sample2.B, sample2.C, sample2.F,
		}
		require.Equal(t, expectedParams, params)
	})

	t.Run("SingleObjectWithSingleColumn", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleSample), []string{"ya"})
		require.Equal(t, []string{}, tags)
		require.Equal(t, []any{sample1.B}, params)
	})

	t.Run("SingleObjectWithMultipleColumns", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleSample), []string{"ya", "pqarray"})
		require.Equal(t, []string{}, tags)
		require.Equal(t, []any{sample1.B, sample1.F}, params)
	})

	t.Run("TwoObjectWithSingleColumn", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(twoSamples), []string{"ya"})
		require.Equal(t, []string{}, tags)
		require.Equal(t, []any{sample1.B, sample2.B}, params)
	})

	t.Run("TwoObjectWithMultipleColumnsWithInverseOrder", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(twoSamples), []string{"pqarray", "ya"})
		require.Equal(t, []string{}, tags)
		require.Equal(t, []any{sample1.F, sample1.B, sample2.F, sample2.B}, params)
	})

	t.Run("SpecifyDbTagOfUnexportedField", func(t *testing.T) {
		fn1 := func() { getParams(reflect.ValueOf(singleSample), []string{"yo"}) }
		fn2 := func() { getParams(reflect.ValueOf(singleSample), []string{"ya", "yo"}) }
		require.Panics(t, fn1)
		require.Panics(t, fn2)
	})

	t.Run("SpecifyNonExistentDbTag", func(t *testing.T) {
		fn1 := func() { getParams(reflect.ValueOf(singleSample), []string{"malon"}) }
		fn2 := func() { getParams(reflect.ValueOf(singleSample), []string{"ya", "whatyouwant"}) }
		require.Panics(t, fn1)
		require.Panics(t, fn2)
	})
}

func TestGenerateInsertQuery(t *testing.T) {
	t.Run("SingleObjectSingleColumn", func(t *testing.T) {
		query := generateInsertQuery(sqlInsertQueryExample, []string{"column1"}, 1)
		expectedQuery := `
INSERT INTO table_name (column1)
VALUES ($1)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("SingleObjectTwoColumns", func(t *testing.T) {
		query := generateInsertQuery(sqlInsertQueryExample, []string{"column1", "column2"}, 1)
		expectedQuery := `
INSERT INTO table_name (column1,column2)
VALUES ($1,$2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("TwoObjectSingleColumn", func(t *testing.T) {
		query := generateInsertQuery(sqlInsertQueryExample, []string{"column1"}, 2)
		expectedQuery := `
INSERT INTO table_name (column1)
VALUES ($1),($2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
	})

	t.Run("TwoObjectsTwoColumns", func(t *testing.T) {
		query := generateInsertQuery(sqlInsertQueryExample, []string{"column1", "column2"}, 2)
		expectedQuery := `
INSERT INTO table_name (column1,column2)
VALUES ($1,$2),($3,$4)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
	})
}

func TestInsertQuery(t *testing.T) {
	t.Run("NoColumns", func(t *testing.T) {
		query, params := InsertQuery(sqlInsertQueryExample, sample1)
		expectedQuery := `
INSERT INTO table_name (ya,bla,pqarray)
VALUES ($1,$2,$3)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.B, sample1.C, sample1.F}, params)
	})

	t.Run("SingleColumn", func(t *testing.T) {
		query, params := InsertQuery(sqlInsertQueryExample, sample1, "pqarray")
		expectedQuery := `
INSERT INTO table_name (pqarray)
VALUES ($1)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.F}, params)
	})

	t.Run("MultipleColumns", func(t *testing.T) {
		query, params := InsertQuery(sqlInsertQueryExample, sample1, "ya", "pqarray")
		expectedQuery := `
INSERT INTO table_name (ya,pqarray)
VALUES ($1,$2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.B, sample1.F}, params)
	})

	t.Run("MultipleColumnsWithInverseOrder", func(t *testing.T) {
		query, params := InsertQuery(sqlInsertQueryExample, sample1, "pqarray", "ya")
		expectedQuery := `
INSERT INTO table_name (pqarray,ya)
VALUES ($1,$2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.F, sample1.B}, params)
	})
}

func TestBatchInsertQuery(t *testing.T) {
	t.Run("SingleObjectNoColumns", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, singleSample)
		expectedQuery := `
INSERT INTO table_name (ya,bla,pqarray)
VALUES ($1,$2,$3)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.B, sample1.C, sample1.F}, params)
	})

	t.Run("SingleObjectSingleColumn", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, singleSample, "pqarray")
		expectedQuery := `
INSERT INTO table_name (pqarray)
VALUES ($1)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.F}, params)
	})

	t.Run("SingleObjectMultipleColumns", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, singleSample, "ya", "pqarray")
		expectedQuery := `
INSERT INTO table_name (ya,pqarray)
VALUES ($1,$2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.B, sample1.F}, params)
	})

	t.Run("SingleObjectMultipleColumnsWithInverseOrder", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, singleSample, "pqarray", "ya")
		expectedQuery := `
INSERT INTO table_name (pqarray,ya)
VALUES ($1,$2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.F, sample1.B}, params)
	})

	t.Run("TwoObjectsNoColumns", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, twoSamples)
		expectedQuery := `
INSERT INTO table_name (ya,bla,pqarray)
VALUES ($1,$2,$3),($4,$5,$6)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		expectedParams := []any{
			sample1.B, sample1.C, sample1.F,
			sample2.B, sample2.C, sample2.F,
		}
		require.Equal(t, expectedParams, params)
	})

	t.Run("TwoObjectsTwoColumn", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, twoSamples, "pqarray")
		expectedQuery := `
INSERT INTO table_name (pqarray)
VALUES ($1),($2)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		require.Equal(t, []any{sample1.F, sample2.F}, params)
	})

	t.Run("TwoObjectsMultipleColumns", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, twoSamples, "ya", "pqarray")
		expectedQuery := `
INSERT INTO table_name (ya,pqarray)
VALUES ($1,$2),($3,$4)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		expectedParams := []any{
			sample1.B, sample1.F,
			sample2.B, sample2.F,
		}
		require.Equal(t, expectedParams, params)
	})

	t.Run("TwoObjectsMultipleColumnsWithInverseOrder", func(t *testing.T) {
		query, params := BatchInsertQuery(sqlInsertQueryExample, twoSamples, "pqarray", "ya")
		expectedQuery := `
INSERT INTO table_name (pqarray,ya)
VALUES ($1,$2),($3,$4)
ON CONFLICT (column_name) DO NOTHING
`
		require.Equal(t, expectedQuery, query)
		expectedParams := []any{
			sample1.F, sample1.B,
			sample2.F, sample2.B,
		}
		require.Equal(t, expectedParams, params)
	})
}

func TestGetParamsWithEmbeddedFields(t *testing.T) {
	wrapperSample := Wrapper{
		Field: "some_value",
		Sample: Sample{
			B: 10,
			C: "embedded",
			F: pq.Int64Array{7, 8, 9},
		},
	}

	singleWrapperSample := []*Wrapper{&wrapperSample}

	t.Run("SingleObjectWithEmbeddedNoColumns", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleWrapperSample), []string{})
		require.Equal(t, []string{"field", "ya", "bla", "pqarray"}, tags)
		expectedParams := []any{wrapperSample.Field, wrapperSample.B, wrapperSample.C, wrapperSample.F}
		require.Equal(t, expectedParams, params)
	})

	t.Run("SingleObjectWithEmbeddedSpecificColumns", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleWrapperSample), []string{"field", "ya", "pqarray"})
		require.Equal(t, []string{}, tags)
		expectedParams := []any{wrapperSample.Field, wrapperSample.B, wrapperSample.F}
		require.Equal(t, expectedParams, params)
	})

	t.Run("EmbeddedFieldTagOverNoEmbeddedFieldTagSpecified", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleWrapperSample), []string{"field"})
		require.Equal(t, []string{}, tags)
		require.Equal(t, []any{wrapperSample.Field}, params)
	})

	t.Run("EmbeddedFieldNotSpecifiedButOtherEmbeddedFieldsAre", func(t *testing.T) {
		tags, params := getParams(reflect.ValueOf(singleWrapperSample), []string{"ya", "bla"})
		require.Equal(t, []string{}, tags)
		expectedParams := []any{wrapperSample.B, wrapperSample.C}
		require.Equal(t, expectedParams, params)
	})

	t.Run("SpecifyNonExistentDbTagInWrapper", func(t *testing.T) {
		fn := func() { getParams(reflect.ValueOf(singleWrapperSample), []string{"not_exist"}) }
		require.Panics(t, fn)
	})
}
