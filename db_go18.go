//go:build go1.8

package txdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"time"
)

func buildRows(r *sql.Rows) (driver.Rows, error) {
	set := &rowSets{}
	rs := &rows{}
	if err := rs.read(r); err != nil {
		return set, err
	}
	set.sets = append(set.sets, rs)
	for r.NextResultSet() {
		rss := &rows{}
		if err := rss.read(r); err != nil {
			return set, err
		}
		set.sets = append(set.sets, rss)
	}
	return set, nil
}

// Implement the "RowsNextResultSet" interface
func (rs *rowSets) HasNextResultSet() bool {
	return rs.pos+1 < len(rs.sets)
}

// Implement the "RowsNextResultSet" interface
func (rs *rowSets) NextResultSet() error {
	if !rs.HasNextResultSet() {
		return io.EOF
	}

	rs.pos++
	return nil
}

func (c *conn) beginTxOnce(ctx context.Context, done <-chan struct{}) (*sql.Tx, error) {
	if c.tx == nil {
		rootCtx, cancel := context.WithCancel(context.Background())
		tx, err := c.drv.db.BeginTx(rootCtx, &sql.TxOptions{})
		if err != nil {
			cancel()
			return nil, err
		}
		c.tx, c.ctx, c.cancel = tx, rootCtx, cancel
	}
	go func() {
		select {
		case <-ctx.Done():
			// operation was interrupted by context cancel, so we cancel parent as well
			c.cancel()
		case <-done:
			// operation was successfully finished, so we don't close ctx on tx
		case <-c.ctx.Done():
		}
	}()
	return c.tx, nil
}

// Implement the "QueryerContext" interface
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.Lock()
	defer c.Unlock()

	done := make(chan struct{})
	defer close(done)

	tx, err := c.beginTxOnce(ctx, done)
	if err != nil {
		return nil, err
	}

	rs, err := tx.QueryContext(ctx, query, mapNamedArgs(args)...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	return buildRows(rs)
}

// Implement the "ExecerContext" interface
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.Lock()
	defer c.Unlock()

	maxRetries := 5
	retryCount := 0
	for {
		done := make(chan struct{})
		tx, err := c.beginTxOnce(ctx, done)
		if err != nil {
			return nil, err
		}

		// errWithSQLState is implemented by pgx (pgconn.PgError) and lib/pq
		type errWithSQLState interface {
			SQLState() string
		}

		c.execHistory = append(c.execHistory, &exec{
			Query: query,
			Args:  args,
		})

		var sqlErr errWithSQLState
		res, err := tx.ExecContext(ctx, query, mapNamedArgs(args)...)
		if err != nil {
			isSQLErr := errors.As(err, &sqlErr)
			if isSQLErr {
				fmt.Printf("%s - sqlErr - %s\n", c.dsn, sqlErr.SQLState())

				if err := tx.Rollback(); err != nil {
					return nil, fmt.Errorf("rollback failed: %w", err)
				}
				if sqlErr.SQLState() != "40001" {
					return nil, err
				}
				tx, err := c.drv.db.Begin()
				if err != nil {
					return nil, fmt.Errorf("start new tx failed: %w", err)
				}
				fmt.Printf("%s - NEW TX \n", c.dsn)
				c.tx = tx
				c.saves = 0
				c.saves++
				id := fmt.Sprintf("tx_%d", c.saves)
				_, err = tx.Exec(c.savePoint.Create(id))
				if err != nil {
					return nil, err
				}
				fmt.Printf("%s - SAVEPOINT %s\n", c.dsn, id)
				c.Unlock()
				for i, eh := range c.execHistory {
					fmt.Printf("%s - apply history - %x\n", c.dsn, i)
					res, err = c.ExecContext(ctx, eh.Query, eh.Args)
				}
				c.Lock()
				return res, err
			}
			retryCount++
			if maxRetries > 0 && retryCount > maxRetries {
				close(done)
				return nil, fmt.Errorf("retrying txn failed after %d attempts. original error: %s", maxRetries, err)
			}
			fmt.Printf("%s - Retrying txn...\n", c.dsn)
			close(done)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		close(done)
		return res, err
	}
}

// Implement the "ConnBeginTx" interface
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}

// Implement the "ConnPrepareContext" interface
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.Lock()
	defer c.Unlock()

	done := make(chan struct{})
	defer close(done)

	tx, err := c.beginTxOnce(ctx, done)
	if err != nil {
		return nil, err
	}

	st, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	stmtFailedStr := make(chan bool)
	go func() {
		select {
		case <-c.ctx.Done():
		case erred := <-stmtFailedStr:
			if erred {
				st.Close()
			}
		}
	}()
	return &stmt{st: st, done: stmtFailedStr}, nil
}

// Implement the "Pinger" interface
func (c *conn) Ping(ctx context.Context) error {
	return c.drv.db.PingContext(ctx)
}

// Implement the "StmtExecContext" interface
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dr, err := s.st.ExecContext(ctx, mapNamedArgs(args)...)
	if err != nil {
		s.closeDone(true)
	}
	return dr, err
}

// Implement the "StmtQueryContext" interface
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.st.QueryContext(ctx, mapNamedArgs(args)...)
	if err != nil {
		s.closeDone(true)
		return nil, err
	}
	return buildRows(rows)
}

func mapNamedArgs(args []driver.NamedValue) (res []interface{}) {
	res = make([]interface{}, len(args))
	for i := range args {
		name := args[i].Name
		if name != "" {
			res[i] = sql.Named(name, args[i].Value)
		} else {
			res[i] = args[i].Value
		}
	}
	return
}
