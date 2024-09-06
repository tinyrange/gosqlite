package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/tinyrange/gosqlite"
)

var (
	input = flag.String("input", "", "The input sqlite file to read.")
)

func appMain() error {
	flag.Parse()

	data, err := os.ReadFile(*input)
	if err != nil {
		return err
	}

	db, err := gosqlite.ParseDatabase(data)
	if err != nil {
		return err
	}

	for _, name := range db.Tables() {
		table, err := db.Table(name)
		if err != nil {
			return err
		}

		slog.Info("table", "name", name, "sql", table.Sql)
		if err := table.Read(func(val []any) error {
			slog.Info("row", "values", val)

			return nil
		}); err != nil {
			return err
		}
	}

	_ = db

	return nil
}

func main() {
	if err := appMain(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
