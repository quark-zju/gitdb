package gitdb

import (
	"encoding/hex"
	"strconv"
)

type treeItem struct {
	Oid  string
	Name string
	Mode int32
}

func (ti *treeItem) IsTree() bool {
	return ti.Mode&0100000 == 0
}

// TODO
func parseTree(body []byte) []*treeItem {
	var result []*treeItem
	for pos, startPos, spacePos := 0, 0, 0; pos < len(body)-20; pos++ {
		switch body[pos] {
		case ' ':
			spacePos = pos
		case 0:
			// mode +     " " + name + "\0" + binOid (20 bytes)
			// ^           ^            ^
			// startPos    spacePos     pos
			if startPos >= spacePos || spacePos+1 >= pos {
				// ignore illegal format
				continue
			}
			mode, _ := strconv.ParseUint(string(body[startPos:spacePos]), 8, 64)
			startPos = pos + 21
			ti := treeItem{
				Oid:  hex.EncodeToString(body[pos+1 : startPos]),
				Name: string(body[spacePos+1 : pos]),
				Mode: int32(mode),
			}
			result = append(result, &ti)
		}
	}
	return result
}
