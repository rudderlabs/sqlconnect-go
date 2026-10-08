package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

var _ sqlconnect.VisibilityChecker = (*DB)(nil)

// visibilitySQL reads the table row on the replica that answers. The setting
// makes that replica apply every committed change first.
const visibilitySQL = "SELECT toString(uuid)\nFROM system.tables\nWHERE database = ? AND name = ?\nSETTINGS select_sequential_consistency = 1"

// Visibility defaults. A DDL change can reach other Cloud replicas late, so
// a miss repeats with backoff until the deadline.
const (
	defaultInitialBackoff = 250 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
	defaultDeadline       = 60 * time.Second
)

// errDeadline tells the callers of poll that the visibility deadline passed.
// It never leaves this file.
var errDeadline = errors.New("visibility deadline passed")

// sleep waits for d or until ctx ends. Tests replace it.
var sleep = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// withDefaults replaces each zero field of p with its default.
func withDefaults(p sqlconnect.VisibilityPolicy) sqlconnect.VisibilityPolicy {
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = defaultInitialBackoff
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = defaultMaxBackoff
	}
	if p.Deadline <= 0 {
		p.Deadline = defaultDeadline
	}
	return p
}

// AwaitTable returns the table's UUID once the table is visible, and with a
// non-empty wantUUID once it has that UUID.
func (db *DB) AwaitTable(ctx context.Context, exec sqlconnect.QueryExecutor, ref sqlconnect.RelationRef, wantUUID string, p sqlconnect.VisibilityPolicy) (string, error) {
	ref, err := db.resolve(ref)
	if err != nil {
		return "", err
	}
	var uuid string
	err = poll(ctx, p, func(vctx context.Context) (bool, error) {
		switch err := exec.QueryRowContext(vctx, visibilitySQL, ref.Schema, ref.Name).Scan(&uuid); {
		case errors.Is(err, sql.ErrNoRows):
			return false, nil
		case err != nil:
			return false, bound("visibility check", "", err)
		}
		return wantUUID == "" || uuid == wantUUID, nil
	})
	if errors.Is(err, errDeadline) {
		state := "visible"
		if wantUUID != "" {
			state = "visible with UUID " + wantUUID
		}
		return "", db.notVisible(ref, state)
	}
	if err != nil {
		return "", err
	}
	return uuid, nil
}

// AwaitTableAbsent returns once the name resolves to no table.
func (db *DB) AwaitTableAbsent(ctx context.Context, exec sqlconnect.QueryExecutor, ref sqlconnect.RelationRef, p sqlconnect.VisibilityPolicy) error {
	ref, err := db.resolve(ref)
	if err != nil {
		return err
	}
	err = poll(ctx, p, func(vctx context.Context) (bool, error) {
		var uuid string
		switch err := exec.QueryRowContext(vctx, visibilitySQL, ref.Schema, ref.Name).Scan(&uuid); {
		case errors.Is(err, sql.ErrNoRows):
			return true, nil
		case err != nil:
			return false, bound("visibility check", "", err)
		}
		return false, nil
	})
	if errors.Is(err, errDeadline) {
		return db.notVisible(ref, "absent")
	}
	return err
}

func (db *DB) notVisible(ref sqlconnect.RelationRef, state string) error {
	return cherr.New(cherr.CodeDDLNotVisible, "",
		fixedMessages[cherr.CodeDDLNotVisible]+": table "+db.QuoteTable(ref)+", expected state "+state)
}

// retryVisibleRead runs a side-effect-free read of a table that an earlier
// check already saw. It retries in place only on a visibility miss (server
// code 60 or 81), inside the same deadline. Any other error returns at once.
// Never pass a write: a failed write may still have written a block.
func retryVisibleRead(ctx context.Context, p sqlconnect.VisibilityPolicy, read func(ctx context.Context) error) error {
	var miss error
	err := poll(ctx, p, func(vctx context.Context) (bool, error) {
		err := read(vctx)
		if err == nil {
			return true, nil
		}
		if c := classify(err).ServerCode; c == 60 || c == 81 {
			miss = err
			return false, nil
		}
		return false, bound("read", "", err)
	})
	if errors.Is(err, errDeadline) {
		return wrap(cherr.CodeDDLNotVisible, "", fixedMessages[cherr.CodeDDLNotVisible]+": the read still found no table", miss)
	}
	return err
}

// poll calls done until it reports success, it fails, or the deadline passes.
// Every read and sleep gets the deadline-bounded context. A caller
// cancellation returns the context error, which is not a visibility verdict.
func poll(ctx context.Context, p sqlconnect.VisibilityPolicy, done func(ctx context.Context) (bool, error)) error {
	p = withDefaults(p)
	vctx, cancel := context.WithTimeout(ctx, p.Deadline)
	defer cancel()
	backoff := min(p.InitialBackoff, p.MaxBackoff)
	for {
		ok, err := done(vctx)
		switch {
		case ok:
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case vctx.Err() != nil:
			return errDeadline
		case err != nil:
			return err
		}
		if err := sleep(vctx, backoff); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errDeadline
		}
		backoff = min(backoff*2, p.MaxBackoff)
	}
}
