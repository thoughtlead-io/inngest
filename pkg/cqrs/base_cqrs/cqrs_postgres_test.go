package base_cqrs

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/cqrs"
	sqlc_psql "github.com/inngest/inngest/pkg/cqrs/base_cqrs/sqlc/postgres"
	sqlc "github.com/inngest/inngest/pkg/cqrs/base_cqrs/sqlc/sqlite"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// EnvTestPostgresURI is the connection string of a disposable Postgres database used by the
// Postgres CQRS tests, eg. "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable".
// The tests are skipped when it is not set, so they do not run in CI unless the job provides it.
//
// WARNING: never point this at a shared or real Inngest database.  The tests run the real
// migrations against it and insert apps, functions and function finishes which are not removed.
//
//	docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16-alpine
//	INNGEST_TEST_POSTGRES_URI='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
//	  go test ./pkg/cqrs/base_cqrs/ -run TestPostgresCQRS
const EnvTestPostgresURI = "INNGEST_TEST_POSTGRES_URI"

// Queries taking a list of IDs must work with any number of IDs.  The postgres variants were
// generated as `IN ($1)` without the slice marker which the generated code expands, so they
// failed with "mismatched param and argument count" unless exactly one ID was given.
func TestPostgresCQRSDeleteFunctionsByIDs(t *testing.T) {
	ctx := context.Background()
	appID := uuid.New()

	cm, _, cleanup := initPostgresCQRS(t, withInitCQRSOptApp(appID))
	defer cleanup()

	for _, count := range []int{0, 1, 3} {
		t.Run(fmt.Sprintf("delete %d functions", count), func(t *testing.T) {
			// Create one more function than we delete, ensuring that it's left alone.
			fnIDs := make([]uuid.UUID, count+1)
			for i := range fnIDs {
				fnIDs[i] = uuid.New()
				_, err := cm.InsertFunction(ctx, cqrs.InsertFunctionParams{
					ID:        fnIDs[i],
					AccountID: uuid.New(),
					EnvID:     uuid.New(),
					AppID:     appID,
					Name:      fmt.Sprintf("Delete %d Function %d", count, i+1),
					Slug:      fmt.Sprintf("delete-%d-function-%d", count, i+1),
					Config:    `{"triggers": [{"event": "delete.id.event"}]}`,
					CreatedAt: time.Now(),
				})
				require.NoError(t, err)
			}

			err := cm.DeleteFunctionsByIDs(ctx, fnIDs[:count])
			require.NoError(t, err)

			for i, fnID := range fnIDs {
				fn, err := cm.GetFunctionByInternalUUID(ctx, fnID)
				require.NoError(t, err)
				assert.Equal(t, i < count, fn.IsArchived())
			}
		})
	}

	t.Run("delete non-existent function IDs", func(t *testing.T) {
		err := cm.DeleteFunctionsByIDs(ctx, []uuid.UUID{uuid.New(), uuid.New()})
		require.NoError(t, err)
	})

	t.Run("delete nil and duplicate function IDs", func(t *testing.T) {
		require.NoError(t, cm.DeleteFunctionsByIDs(ctx, nil))

		fnID := uuid.New()
		_, err := cm.InsertFunction(ctx, cqrs.InsertFunctionParams{
			ID:        fnID,
			AccountID: uuid.New(),
			EnvID:     uuid.New(),
			AppID:     appID,
			Name:      "Delete Duplicate Function",
			Slug:      "delete-duplicate-function",
			Config:    `{"triggers": [{"event": "delete.id.event"}]}`,
			CreatedAt: time.Now(),
		})
		require.NoError(t, err)

		err = cm.DeleteFunctionsByIDs(ctx, []uuid.UUID{fnID, fnID, fnID})
		require.NoError(t, err)

		fn, err := cm.GetFunctionByInternalUUID(ctx, fnID)
		require.NoError(t, err)
		assert.True(t, fn.IsArchived())
	})

	t.Run("delete within a transaction", func(t *testing.T) {
		// App registration removes unseen functions within a tx.
		tx, err := cm.WithTx(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()

		err = tx.DeleteFunctionsByIDs(ctx, []uuid.UUID{uuid.New(), uuid.New()})
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	})
}

func TestPostgresCQRSGetFunctionRunFinishesByRunIDs(t *testing.T) {
	ctx := context.Background()

	cm, q, cleanup := initPostgresCQRS(t)
	defer cleanup()

	runIDs := make([]ulid.ULID, 4)
	for i := range runIDs {
		runIDs[i] = ulid.Make()
		err := q.InsertFunctionFinish(ctx, sqlc.InsertFunctionFinishParams{
			RunID:              runIDs[i],
			Status:             sql.NullString{Valid: true, String: "Completed"},
			Output:             sql.NullString{Valid: true, String: `{"ok":true}`},
			CompletedStepCount: sql.NullInt64{Valid: true, Int64: 1},
			CreatedAt:          sql.NullTime{Valid: true, Time: time.Now()},
		})
		require.NoError(t, err)
	}

	w, ok := cm.(wrapper)
	require.True(t, ok)

	for _, count := range []int{0, 1, 3} {
		t.Run(fmt.Sprintf("get %d finishes", count), func(t *testing.T) {
			finishes, err := w.GetFunctionRunFinishesByRunIDs(ctx, uuid.Nil, uuid.Nil, runIDs[:count])
			require.NoError(t, err)

			found := make([]ulid.ULID, len(finishes))
			for i, f := range finishes {
				found[i] = f.RunID
			}
			assert.ElementsMatch(t, runIDs[:count], found)
		})
	}
}

func initPostgresCQRS(t *testing.T, opts ...withInitCQRSOpt) (cqrs.Manager, sqlc.Querier, func()) {
	ctx := context.Background()

	uri := os.Getenv(EnvTestPostgresURI)
	if uri == "" {
		t.Skipf("%s is not set, skipping postgres test", EnvTestPostgresURI)
	}

	opt := initCQRSOpt{}
	for _, apply := range opts {
		apply(&opt)
	}

	// New() keeps a singleton postgres handle, so open a handle per test and migrate it directly.
	db, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	require.NoError(t, up(db, BaseCQRSOptions{PostgresURI: uri}))

	cm := NewCQRS(db, "postgres", sqlc_psql.NewNormalizedOpts{})
	q := NewQueries(db, "postgres", sqlc_psql.NewNormalizedOpts{})

	cleanup := func() {
		db.Close()
	}

	if opt.appID != uuid.Nil {
		_, err := cm.UpsertApp(ctx, cqrs.UpsertAppParams{
			ID:          opt.appID,
			Name:        fmt.Sprintf("app:%s", opt.appID),
			SdkLanguage: "go",
			SdkVersion:  "1.2.3",
			Framework:   sql.NullString{Valid: true, String: "gin"},
			Metadata:    `{"environment": "test", "version": "1.0"}`,
			AppVersion:  "v2.1.0",
		})
		require.NoError(t, err)
	}

	return cm, q, cleanup
}
