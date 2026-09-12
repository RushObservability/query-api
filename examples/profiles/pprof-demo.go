// A local CPU workload with a pprof endpoint for the collector example.
// Run with: go run examples/profiles/pprof-demo.go
package main

import (
    "crypto/sha256"
    "log"
    "net/http"
    _ "net/http/pprof"
    "time"
)

var digest [32]byte

func work() {
    input := []byte("Rush CPU profiling example")
    for i := 0; i < 100000; i++ {
        digest = sha256.Sum256(input)
        input = digest[:]
    }
}

func main() {
    go func() {
        for {
            work()
            time.Sleep(100 * time.Millisecond)
        }
    }()
    log.Print("Local profiling demo on 127.0.0.1:6062; Ctrl-C stops it")
    log.Fatal(http.ListenAndServe("127.0.0.1:6062", nil))
}
