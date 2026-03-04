package util

import "github.com/google/uuid"

func NewID(prefix string) string {
	return prefix + "_" + uuid.NewString()
}