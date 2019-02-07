package main

import (
	"os"
	"strconv"
	"strings"
)

type dbgVar struct {
	name  string
	value *int32
}

// Holds variables parsed from DBPERFDEBUG env var, variables can be string or int32
var debug struct {
	pprof int32
}

var dbgvars = []dbgVar{
	{"pprof", &debug.pprof},
}

func init() {
	for p := os.Getenv("DBPERFDEBUG"); p != ""; {
		field := ""
		i := strings.Index(p, ",")
		if i < 0 {
			field, p = p, ""
		} else {
			field, p = p[:i], p[i+1:]
		}
		i = strings.Index(field, "=")
		if i < 0 {
			continue
		}
		key, value := field[:i], field[i+1:]

		for _, v := range dbgvars {
			if v.name == key {
				if n, err := strconv.Atoi(value); err == nil {
					*v.value = int32(n)
				}
			}
		}
	}
}
