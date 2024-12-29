package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strings"
)

type Data struct {
	minimum, maximum, total, count int
}

func main() {
	f, err := os.Open("../measurements.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	m := map[string]Data{}

	scan := bufio.NewScanner(f)
	for scan.Scan() {
		line := scan.Text()
		spl := strings.Split(line, ";")
		name := spl[0]
		num := []byte(spl[1])
		neg := 1
		val := 0
		for _, d := range num {
			switch d {
			case '-':
				neg = -1
			case '.':
			default:
				val *= 10
				val += int(d - '0')

			}
		}
		val *= neg

		prev, ok := m[name]
		if ok {
			prev.minimum = min(prev.minimum, val)
			prev.maximum = max(prev.maximum, val)
			prev.total += val
			prev.count++
		} else {
			prev = Data{minimum: val, maximum: val, total: val, count: 1}
		}
		m[name] = prev
	}

	names := make([]string, len(m))
	i := 0
	for k := range m {
		names[i] = k
		i++
	}

	slices.Sort(names)

	fmt.Print("{")
	comma := ""
	for _, name := range names {
		this := m[name]
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
