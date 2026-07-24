package pgx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNoRows occurs when QueryRow returns no rows.
var ErrNoRows = errors.New("no rows in result set")

// Rows is the result set of a query.
type Rows interface {
	// Close closes the rows, making the connection ready for another query.
	// Close is safe to call multiple times.
	Close()

	// Err returns the error, if any, that was encountered during iteration.
	// Err may be called after Close.
	Err() error

	// CommandTag returns the command tag from the query.
	CommandTag() pgconn.CommandTag

	// FieldDescriptions returns the field descriptions for the rows.
	FieldDescriptions() []pgconn.FieldDescription

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available or an error occurred. It
	// automatically closes the rows when all rows are read or an error occurs.
	Next() bool

	// Scan reads the values from the current row into dest. dest can include
	// pointers to core types, values implementing the Scanner interface, and nil.
	// nil will skip the value.
	Scan(dest ...any) error

	// Values returns the decoded row values.
	Values() ([]any, error)

	// Conn returns the connection the query was executed on.
	Conn() *Conn
}

type connRows struct {
	ctx          context.Context
	conn         *Conn
	resultReader *pgconn.MultiResultReader
	rowsResult   *pgconn.RowsResult
	err          error
	closed       bool
}

func (rows *connRows) Close() {
	if rows.closed {
		return
	}

	rows.closed = true

	if rows.resultReader != nil {
		if rows.ctx != nil && rows.ctx.Err() != nil && rows.conn != nil {
			rows.conn.Close(context.Background())
		}
		err := rows.resultReader.Close()
		if err != nil && rows.err == nil {
			rows.err = err
		}
	}
}

func (rows *connRows) Err() error {
	return rows.err
}

func (rows *connRows) CommandTag() pgconn.CommandTag {
	if rows.rowsResult == nil {
		return pgconn.CommandTag{}
	}
	return rows.rowsResult.CommandTag()
}

func (rows *connRows) FieldDescriptions() []pgconn.FieldDescription {
	if rows.rowsResult == nil {
		return nil
	}
	return rows.rowsResult.FieldDescriptions()
}

func (rows *connRows) Next() bool {
	if rows.closed {
		return false
	}

	if rows.rowsResult != nil {
		if rows.rowsResult.Next() {
			return true
		}
		if err := rows.rowsResult.Err(); err != nil {
			rows.err = err
		}
	}

	if rows.resultReader != nil {
		var err error
		rows.rowsResult, err = rows.resultReader.NextRowGroup()
		if err != nil {
			if rows.err == nil {
				rows.err = err
			}
			rows.Close()
			return false
		}
		return rows.Next()
	}

	rows.Close()
	return false
}

func (rows *connRows) Scan(dest ...any) error {
	if rows.closed {
		return errors.New("rows are closed")
	}
	if rows.rowsResult == nil {
		return errors.New("no row data available")
	}
	return rows.rowsResult.Scan(dest...)
}

func (rows *connRows) Values() ([]any, error) {
	if rows.closed {
		return nil, errors.New("rows are closed")
	}
	if rows.rowsResult == nil {
		return nil, errors.New("no row data available")
	}
	return rows.rowsResult.Values()
}

func (rows *connRows) Conn() *Conn {
	return rows.conn
}

// Row is a single row result from a query.
type Row interface {
	// Scan scans the row. See Rows.Scan for details. If no rows were found,
	// ErrNoRows is returned.
	Scan(dest ...any) error
}

type connRow connRows

func (r *connRow) Scan(dest ...any) error {
	rows := (*connRows)(r)

	if rows.closed {
		return errors.New("row is closed")
	}

	if !rows.Next() {
		if rows.err != nil {
			return rows.err
		}
		return ErrNoRows
	}
	defer rows.Close()

	return rows.Scan(dest...)
}
