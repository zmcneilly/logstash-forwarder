package lsf

import (
	"os"
)

// -----------------------------------------------------------------------------
// Pipeline & Registry objects
// -----------------------------------------------------------------------------

// Captures a specific file event.
// Used as message in pipeline and entries in registrar file
type FileEvent struct {
	Source *string `json:"source,omitempty"`
	Offset int64   `json:"offset,omitempty"`
	Line   uint64  `json:"line,omitempty"`
	Text   *string `json:"text,omitempty"`
	Fields *map[string]string

	fileinfo *os.FileInfo
}
