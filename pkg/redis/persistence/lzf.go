package redis_persistence

import "errors"

/**
 * LZF compression/decompression module. Ported from the C
 * implementation of liblzf, specfically lzf_c.c and lzf_d.c
 * @license BSD-2-Clause
 * @author McSimp
 * @url https://github.com/McSimp/lzfjs
 */

/**
 * Then I ported  js implementation from lzfjs to golang
 * lzfjs: https://github.com/McSimp/lzfjs
 */

func LZFDecompress(data []byte) ([]byte, error) {
	length := len(data)
	input := data
	output := make([]byte, length*2)

	ip := 0
	op := 0

	for {
		ctrl := input[ip]
		ip++

		if ctrl < (1 << 5) { /* literal run */
			ctrl++

			if ip+int(ctrl) > length {
				return nil, errors.New("invalid-input")
			}

			for ctrl > 0 {
				ctrl--
				output[op] = input[ip]
				op++
				ip++
			}
		} else { /* back reference */
			len := ctrl >> 5
			ref := op - ((int(ctrl) & 0x1f) << 8) - 1

			if ip >= length {
				return nil, errors.New("invalid-input")
			}

			if len == 7 {
				len += input[ip]
				ip++

				if ip >= length {
					return nil, errors.New("invalid-input")
				}
			}

			ref -= int(input[ip])
			ip++

			if ref < 0 {
				return nil, errors.New("invalid-input")
			}

			len += 2

			for {
				output[op] = output[ref]
				op++
				ref++
				len--
				if len == 0 {
					break
				}
			}
		}

		if ip < length {
			break
		}
	}

	return output, nil
}

const (
	hLog      = 16
	hSize     = (1 << hLog)
	lzfMaxOff = (1 << 13)
	lzfMaxRef = ((1 << 8) + (1 << 3))
	lzfMaxLit = (1 << 5)
)

func FRST(data []byte, p int) int {
	return (int(data[p]) << 8) | int(data[p+1])
}

func NEXT(v int, data []byte, p int) int {
	return (v << 8) | int(data[p+2])
}

func IDX(h int) int {
	return (((h * 0x1e35a7bd) >> (32 - hLog - 8)) & (hSize - 1))
}

func LZFCompress(data []byte) ([]byte, error) {
	length := len(data)
	input := data
	output := make([]byte, length*2)
	htab := make([]int, hSize)

	in_end := length
	ip := 0
	hval := FRST(input, ip)
	op := 1
	var lit byte = 0 /* start run */

	for ip < in_end-2 {
		hval = NEXT(hval, data, ip)
		hslot := IDX(hval)
		ref := htab[hslot]
		htab[hslot] = ip

		off := ip - ref - 1

		if ref < ip &&
			off < lzfMaxOff &&
			ref > 0 &&
			input[ref+2] == input[ip+2] &&
			input[ref+1] == input[ip+1] &&
			input[ref] == input[ip] {
			/* match found at *ref++ */
			len := 2
			maxlen := in_end - ip - len
			if maxlen > lzfMaxRef {
				maxlen = lzfMaxRef
			}

			output[op-int(lit)-1] = (lit - 1) & 255 /* stop run */
			if lit == 0 {
				op -= 1 /* undo run if length is zero */
			}

			for {
				len++
				if len < maxlen && input[ref+len] == input[ip+len] {
					break
				}
			}

			len -= 2 /* len is now #octets - 1 */
			ip++

			if len < 7 {
				output[op] = byte(((off >> 8) + (len << 5)) & 0b11111111)
				op++
			} else {
				output[op] = byte(((off >> 8) + (7 << 5)) & 0b11111111)
				op++
				output[op] = byte((len - 7) & 0b11111111)
				op++
			}

			output[op] = byte(off & 0b11111111)
			op++

			lit = 0
			op++ /* start run */

			ip += len + 1

			if ip >= in_end-2 {
				break
			}

			ip -= 2
			hval = FRST(input, ip)

			hval = NEXT(hval, input, ip)
			htab[IDX(hval)] = ip
			ip++

			hval = NEXT(hval, input, ip)
			htab[IDX(hval)] = ip
			ip++
		} else {
			lit++
			output[op] = input[ip]
			op++
			ip++

			if lit == lzfMaxLit {
				output[op-int(lit)-1] = (lit - 1) & 255 /* stop run */
				lit = 0
				op++ /* start run */
			}
		}
	}

	for ip < in_end {
		lit++
		output[op] = input[ip]
		op++
		ip++

		if lit == lzfMaxLit {
			output[op-int(lit)-1] = (lit - 1) & 255 /* stop run */
			lit = 0
			op++ /* start run */
		}
	}

	if lit != 0 {
		output[op-int(lit)-1] = (lit - 1) & 255 /* stop run */
	}

	return output, nil
}
