package utils

import (
	"math/rand"
	"strconv"
)

var letters = ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func HexToBin(hex string) ([]byte, error) {
	binary := make([]byte, len(hex)/2)

	for i := 0; i < len(binary); i++ {
		// byte can be represented by 2 hex digits
		ui, err := strconv.ParseUint(hex[2*i:2*i+2], 16, 8)
		if err != nil {
			return nil, err
		}
		binary[i] = byte(ui & 0xff)
	}

	return binary, nil
}
