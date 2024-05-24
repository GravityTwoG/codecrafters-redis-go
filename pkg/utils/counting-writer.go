package utils

import "io"

type CountingWriter struct {
	io.Writer
	N int
}

func (w *CountingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.N += n
	return n, err
}
