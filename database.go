package gosqlite

import (
	"encoding/binary"
	"fmt"
	"slices"
)

var endian = binary.BigEndian

type BinaryReader []byte

func (r BinaryReader) read(off, len int64) []byte { return r[off : off+len] }
func (r BinaryReader) u16(off int64) uint16       { return endian.Uint16(r.read(off, 2)) }
func (r BinaryReader) u32(off int64) uint32       { return endian.Uint32(r.read(off, 4)) }
func (r BinaryReader) u64(off int64) uint64       { return endian.Uint64(r.read(off, 8)) }
func (r BinaryReader) u8(off int64) uint8         { return uint8(r[off]) }

func (r BinaryReader) u24(off int64) uint32 {
	var b [4]byte

	copy(b[:], r[off:off+3])

	return endian.Uint32(b[:])
}

func (r BinaryReader) u48(off int64) uint64 {
	var b [8]byte

	copy(b[:], r[off:off+6])

	return endian.Uint64(b[:])
}

func (r BinaryReader) decodeVarint(off int64, slice []byte) (uint64, int64) {
	var ret uint64

	for i := 0; i < len(slice); i++ {
		val := slice[i]
		ret |= uint64(val&0b0111_1111) << (i * 7)
	}

	return ret, off + int64(len(slice))
}

func (r BinaryReader) varint(off int64) (uint64, int64) {
	var i int64 = 0

	for i = 0; i < 9; i++ {
		if off+i >= int64(len(r)) {
			return 0, -1 // overflow
		}

		if r[off+i]&0b1000_0000 == 0 {
			break
		}
	}

	i += 1

	data := make([]byte, i)
	copy(data, r[off:off+i])

	slices.Reverse(data)

	return r.decodeVarint(off, data)
}

type Table struct {
	db       *SQLiteDatabase
	Name     string
	rootPage uint64
	Sql      string
}

func (t *Table) Read(cb func(val []any) error) error {
	return t.db.readPage(int(t.rootPage), func(rowId uint64, payload BinaryReader) error {
		// Read the cell header length.
		hdrLen, payloadOff := payload.varint(0)
		if payloadOff == -1 {
			return fmt.Errorf("[outer] overflow reading page")
		}
		if hdrLen > uint64(len(payload)) {
			return fmt.Errorf("hdrLen is longer than the payload %d > %d: %+v", hdrLen, len(payload), payload)
		}

		var (
			types []uint64
			typ   uint64
		)

		for {
			if payloadOff >= int64(hdrLen) {
				break
			}

			typ, payloadOff = payload.varint(payloadOff)
			if payloadOff == -1 {
				return fmt.Errorf("[inner] overflow reading page")
			}

			types = append(types, typ)
		}

		var values []any

		for _, typ := range types {
			if typ == 0 {
				values = append(values, nil)
			} else if typ == 1 {
				values = append(values, uint64(payload.u8(payloadOff)))

				payloadOff += 1
			} else if typ == 2 {
				values = append(values, uint64(payload.u16(payloadOff)))

				payloadOff += 2
			} else if typ == 3 {
				values = append(values, uint64(payload.u24(payloadOff)))

				payloadOff += 3
			} else if typ == 4 {
				values = append(values, uint64(payload.u32(payloadOff)))

				payloadOff += 4
			} else if typ == 5 {
				values = append(values, uint64(payload.u48(payloadOff)))

				payloadOff += 6
			} else if typ == 6 {
				values = append(values, uint64(payload.u64(payloadOff)))

				payloadOff += 8
			} else if typ == 8 {
				values = append(values, uint64(0))
			} else if typ == 9 {
				values = append(values, uint64(1))
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

		if err := cb(values); err != nil {
			return err
		}

		return nil
	})
}

type SQLiteDatabase struct {
	BinaryReader
	pageSize uint16
	tables   map[string]*Table
}

func (db *SQLiteDatabase) readPage(page int, cbCell func(rowId uint64, r BinaryReader) error) error {
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

	pageOffset += 8

	var rightMostPointer uint32 = 0
	if bTreePageType == 0x05 {
		rightMostPointer = db.u32(pageOffset)
		pageOffset += 4
	}

	cellPointers := make([]uint16, numberOfCells)
	for i := 0; i < len(cellPointers); i++ {
		cellPointers[i] = db.u16(pageOffset + int64(i)*2)
	}

	switch bTreePageType {
	case 0x02: // index interior
		return nil
	case 0x05: // table interior cell
		for _, cellPointer := range cellPointers {
			var off = rawPageOffset + int64(cellPointer)

			leftMostPointer := db.u32(off)
			off += 4
			key, _ := db.varint(off)

			_ = key

			for page := leftMostPointer; page <= rightMostPointer; page++ {
				if err := db.readPage(int(page), cbCell); err != nil {
					return err
				}
			}
		}

		return nil
	case 0x0a: // index leaf
		return nil
	case 0x0d: // table exterior cell
		for _, cellPointer := range cellPointers {
			var off = rawPageOffset + int64(cellPointer)

			cellSize, off := db.varint(off)
			rowId, off := db.varint(off)

			payload := BinaryReader(db.read(off, int64(cellSize)))

			if err := cbCell(rowId, payload); err != nil {
				return err
			}
		}

		return nil
	default:
		return fmt.Errorf("unknown bTreePageType: %x", bTreePageType)
	}
}

func (db *SQLiteDatabase) Tables() []string {
	var ret []string

	for k := range db.tables {
		ret = append(ret, k)
	}

	return ret
}

func (db *SQLiteDatabase) Table(name string) (*Table, error) {
	tbl, ok := db.tables[name]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", name)
	}

	return tbl, nil
}

func ParseDatabase(data []byte) (*SQLiteDatabase, error) {
	db := &SQLiteDatabase{BinaryReader: data, tables: make(map[string]*Table)}

	if string(db.read(0, 16)) != "SQLite format 3\x00" {
		return nil, fmt.Errorf("bad magic")
	}

	db.pageSize = db.u16(16)

	schemaTable := &Table{db: db, rootPage: 1}

	if err := schemaTable.Read(func(val []any) error {
		if val[0].(string) != "table" {
			return nil
		}

		db.tables[val[1].(string)] = &Table{
			db:       db,
			rootPage: val[3].(uint64),
			Sql:      val[4].(string),
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return db, nil
}
