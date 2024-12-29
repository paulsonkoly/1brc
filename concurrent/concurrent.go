package main

import (
	"fmt"
	"io"
	"os"
	"reflect"

	// "runtime/pprof"
	"slices"
)

const (
	// ChunkSize is the approximate size of a chunk, it might be bigger spanning upto the next new line.
	ChunkSize = 16 * 1024 * 1024
	WorkPool  = 16
	Filename  = "../measurements.txt"
)

type Data struct {
	minimum, maximum, total, count int
	name                           string
}

// Dict is the main calculation result. It maps from a uint64 to a partial
// result for a given station. It would be more natural to map from the station
// name, but that would require a string allocation per input line. This only
// allocates the name of the station per unique station per chunk processing.
type Dict map[uint64]*Data

// Chunk is a chunk of the input file, from a given position to a given byte
// size. It starts at first character of a line and ends on a newline.
type Chunk struct {
	pos, size int64
}

func main() {
	// cpu, err := os.Create("concurrent.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// err = pprof.StartCPUProfile(cpu)
	// if err != nil {
	// 	panic(err)
	// }
	// defer pprof.StopCPUProfile()

	out := make([]chan Dict, WorkPool)
	for i := range WorkPool {
		out[i] = make(chan Dict, 1)
	}

	chks := chunks(splits())

	f, err := os.Open(Filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for i := range WorkPool {
		go func() {
			args := []Chunk{}
      // send chunk[j] to worker i if j % WorkPool == i
			for j := i; j < len(chks); j += WorkPool {
				args = append(args, chks[j])
			}

			out[i] <- readChunks(f, args)
			close(out[i])
		}()
	}

	cases := make([]reflect.SelectCase, WorkPool)
	for i := range WorkPool {
		cases[i] = reflect.SelectCase{
			Chan: reflect.ValueOf(out[i]),
			Dir:  reflect.SelectRecv,
		}
	}

  // all results
	m := make(Dict)

	closed := 0
	for closed < WorkPool {
		i, recv, ok := reflect.Select(cases)
		if !ok {
			cases = slices.Delete(cases, i, i+1)
			closed++
			continue
		}
		if recvm, ok := recv.Interface().(Dict); ok {
			for k, v := range recvm {
				md, ok := m[k]
				if !ok {
					m[k] = v
				} else {
					md.count += v.count
					md.total += v.total
					md.minimum = min(v.minimum, md.minimum)
					md.maximum = max(v.maximum, md.maximum)
				}
			}
		} else {
			panic("garbage received")
		}
	}

	// list of all names
	names := make([]string, len(m))
	// inverse map from name to Data
	imap := make(map[string]*Data)
	i := 0
	for _, v := range m {
		names[i] = v.name
		imap[v.name] = v
		i++
	}

	slices.Sort(names)

	fmt.Print("{")
	comma := ""
	for _, name := range names {
		this := imap[name]
		fmt.Printf("%s%s %.1f/%.1f/%.1f",
			comma,
			name,
			float64(this.minimum)/10,
			float64(this.total)/(10.*float64(this.count)),
			float64(this.maximum)/10.)
		comma = ", "
	}
	fmt.Print("}")
}

// splits are the split points in bytes falling on newline boundaries.
// costs in this function are amortized by the rest
func splits() []int64 {
	f, err := os.Open(Filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}

	result := make([]int64, 0, fi.Size()/ChunkSize)
	curr := int64(0)
	result = append(result, curr)

	for {
		if curr >= fi.Size() {
			return result
		}

		curr, err = f.Seek(ChunkSize, io.SeekCurrent)
		if err != nil {
			panic(err)
		}

		extra := int64(0)
		r := [1]byte{}
		for {
			nBytes, err := f.Read(r[:])
			if nBytes == 1 {
				extra++
				if r[0] == '\n' {
					break
				}
			} else {
				if err == io.EOF {
					curr = fi.Size()
					extra = 0
					break
				}
				if err != nil {
					panic(err)
				}
			}
		}

		curr += extra
		result = append(result, curr)
	}
}

// chunks converts split points into Chunks
func chunks(points []int64) []Chunk {
	result := make([]Chunk, 0, len(points)-1)
	for i := 0; i < len(points)-1; i++ {
		chunk := Chunk{pos: points[i], size: points[i+1] - points[i]}

		result = append(result, chunk)
	}

	return result
}

// readChunks reads a set of chunks from the file, creates a Dict for its partial result.
func readChunks(f io.ReaderAt, chunks []Chunk) Dict {
	m := make(Dict)

	maxChunkSize := int64(0)
	for _, chunk := range chunks {
		if chunk.size > maxChunkSize {
			maxChunkSize = chunk.size
		}
	}

	buffer := make([]byte, maxChunkSize)

	for _, chunk := range chunks {
		n, err := f.ReadAt(buffer[:chunk.size], chunk.pos)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if int64(n) != chunk.size {
			panic("couldn't read expected chunk size")
		}

		tail := buffer[:chunk.size]
		for len(tail) > 0 {
			var (
				name []byte
				hash uint64
			)

			for i, c := range tail {
				if c == ';' {
					name = tail[:i]
					tail = tail[i+1:]
					break
				}
				hash <<= 5 // All I want here is a unique uint64 per name
				hash |= uint64(c)
			}

			var val int
			neg := 1

			for i, c := range tail {
				switch c {

				case '.':

				case '-':
					neg = -1

				case '\n':
					tail = tail[i+1:]
					goto end

				default:
					val *= 10
					val += int(c - '0')
				}
			}

		end:
			val *= neg

			// this is the main cost centre. we could replace the built in hash with
			// our own. This is what other faster solutions did here.
			md, ok := m[hash]
			if !ok {
				m[hash] = &Data{minimum: val, maximum: val, total: val, count: 1, name: string(name)}
			} else {
				md.minimum = min(md.minimum, val)
				md.maximum = max(md.maximum, val)
				md.total += val
				md.count++
			}
		}
	}
	return m
}
