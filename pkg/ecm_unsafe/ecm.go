package ecm_unsafe

import (
	"reflect"
	"unsafe"
)

// This function avoids allocation that happen when doing []byte(s), making it significantly faster
func UnsafeGetBytes(s string) []byte {
	return (*[0x7fff0000]byte)(unsafe.Pointer(
		(*reflect.StringHeader)(unsafe.Pointer(&s)).Data),
	)[:len(s):len(s)]
}
