package dsql

import (
	"fmt"
	"strings"
)

// chasm_path is text, which can't hold NUL, while CHASM's path encoder keeps a NUL from
// a node's key. The plugin stores 0x00 as 0x01 0x01 and 0x01 as 0x01 0x02. These sort
// before every other byte, as 0x00 and 0x01 do, and neither is a prefix of the other,
// so stored paths keep the encoder's byte order and prefixes.
const (
	chasmPathEscape  = 0x01
	chasmPathNUL     = 0x01
	chasmPathLiteral = 0x02
)

func encodeChasmPath(path string) string {
	if strings.IndexByte(path, 0x00) < 0 && strings.IndexByte(path, chasmPathEscape) < 0 {
		return path
	}
	var b strings.Builder
	b.Grow(len(path) + 2)
	for i := 0; i < len(path); i++ {
		switch c := path[i]; c {
		case 0x00:
			b.WriteByte(chasmPathEscape)
			b.WriteByte(chasmPathNUL)
		case chasmPathEscape:
			b.WriteByte(chasmPathEscape)
			b.WriteByte(chasmPathLiteral)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func encodeChasmPaths(paths []string) []string {
	encoded := make([]string, len(paths))
	for i, path := range paths {
		encoded[i] = encodeChasmPath(path)
	}
	return encoded
}

func decodeChasmPath(stored string) (string, error) {
	if strings.IndexByte(stored, chasmPathEscape) < 0 {
		return stored, nil
	}
	var b strings.Builder
	b.Grow(len(stored))
	for i := 0; i < len(stored); i++ {
		if stored[i] != chasmPathEscape {
			b.WriteByte(stored[i])
			continue
		}
		if i++; i == len(stored) {
			return "", fmt.Errorf("stored CHASM path %q ends with an escape", stored)
		}
		switch stored[i] {
		case chasmPathNUL:
			b.WriteByte(0x00)
		case chasmPathLiteral:
			b.WriteByte(chasmPathEscape)
		default:
			return "", fmt.Errorf("stored CHASM path %q has an unknown escape", stored)
		}
	}
	return b.String(), nil
}
