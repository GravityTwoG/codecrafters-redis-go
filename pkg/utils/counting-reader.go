package utils

import "io"

type CountingReader struct {
	io.Reader
	N int
}

func (w *CountingReader) Read(p []byte) (int, error) {
	n, err := w.Reader.Read(p)
	w.N += n
	return n, err
}
