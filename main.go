package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"os"
)

var endian = binary.BigEndian

type BinaryReader []byte

func (r BinaryReader) read(off, len int64) []byte { return r[off : off+len] }
func (r BinaryReader) u16(off int64) uint16       { return endian.Uint16(r.read(off, 2)) }
func (r BinaryReader) u32(off int64) uint32       { return endian.Uint32(r.read(off, 4)) }
func (r BinaryReader) u64(off int64) uint64       { return endian.Uint64(r.read(off, 8)) }
func (r BinaryReader) u8(off int64) uint8         { return uint8(r[off]) }

func (r BinaryReader) varint(off int64) (uint64, int64) {
	var ret uint64

	var i int64 = 0

	for i = 0; i < 9; i++ {
		val := r.u8(off + i)

		ret |= uint64(val&0b0111_1111) << (i * 8)

		if val&0b1000_0000 == 0 {
			break
		}
	}

	i += 1

	return ret, off + i
}

type SQLiteDatabase struct {
	BinaryReader
	pageSize uint16
}

func (db *SQLiteDatabase) readPage(page int) error {
	var rawPageOffset int64 = (int64(page) - 1) * int64(db.pageSize)

	pageOffset := rawPageOffset

	if page == 1 {
		pageOffset += 100
	}

	bTreePageType := db.u8(pageOffset)
	// firstFreeBlock := db.u16(pageOffset + 1)
	numberOfCells := db.u16(pageOffset + 3)
	// startOfCellContent := db.u16(pageOffset + 5)
	// fragmentedFreeBytes := db.u8(pageOffset + 7)

	cellPointers := make([]uint16, numberOfCells)
	for i := 0; i < len(cellPointers); i++ {
		cellPointers[i] = db.u16(pageOffset + 8 + int64(i)*2)
	}

	switch bTreePageType {
	case 0x0d:
		for _, cellPointer := range cellPointers {
			var off = rawPageOffset + int64(cellPointer)

			cellSize, off := db.varint(off)
			rowId, off := db.varint(off)

			payload := BinaryReader(db.read(off, int64(cellSize)))

			// Read the cell header length.
			hdrLen, payloadOff := payload.varint(0)

			var (
				types []uint64
				typ   uint64
			)

			for {
				if payloadOff >= int64(hdrLen) {
					break
				}

				typ, payloadOff = payload.varint(payloadOff)

				types = append(types, typ)
			}

			var values []any

			for _, typ := range types {
				if typ == 1 {
					values = append(values, payload.u8(payloadOff))

					payloadOff += 1
				} else if typ == 2 {
					values = append(values, payload.u16(payloadOff))

					payloadOff += 2
				} else if typ >= 12 && typ%2 == 0 {
					length := (typ - 12) / 2

					values = append(values, payload.read(payloadOff, int64(length)))

					payloadOff += int64(length)
				} else if typ >= 13 && typ%2 == 1 {
					length := (typ - 13) / 2

					values = append(values, string(payload.read(payloadOff, int64(length))))

					payloadOff += int64(length)
				} else {
					return fmt.Errorf("unknown value type: %d", typ)
				}
			}

			// Write the cell information.
			slog.Info("cell", "rowId", rowId, "values", values)
		}

		return nil
	default:
		return fmt.Errorf("unknown bTreePageType: %x", bTreePageType)
	}
}

func ParseDatabase(data []byte) (*SQLiteDatabase, error) {
	db := &SQLiteDatabase{BinaryReader: data}

	if string(db.read(0, 16)) != "SQLite format 3\x00" {
		return nil, fmt.Errorf("bad magic")
	}

	db.pageSize = db.u16(16)

	if err := db.readPage(1); err != nil {
		return nil, err
	}

	if err := db.readPage(2); err != nil {
		return nil, err
	}

	// Read first page.

	return db, nil
}

var (
	input = flag.String("input", "", "The input sqlite file to read.")
)

func appMain() error {
	flag.Parse()

	data, err := os.ReadFile(*input)
	if err != nil {
		return err
	}

	db, err := ParseDatabase(data)
	if err != nil {
		return err
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
