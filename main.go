package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/mpilar/go-ecm-sketch/pkg/ecm"
)

var (
	TENANTS      = 1000000
	LowHashNum   = 4
	HighHashNum  = 4
	Requests     = int64(WindowSize * WindowFactor)
	WindowSize   = 60
	WindowFactor = 1000000
	//Address = "0.0.0.0:8080"
	Address = "127.0.0.1:8080"
)

func tn(i int) string {
	return fmt.Sprintf("t%06d", i)
}

func main() {
	profile_file := fmt.Sprintf("ecm-sketch-profile-%d.prof", time.Now().Unix())
	f, err := os.Create(profile_file)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Profiling into: %s\n", profile_file)
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	fmt.Println("starting")
	rand.Seed(time.Now().UnixNano())
	//tenants := []string{"t0"}
	counts := make(map[string]int)
	// for i := 1; i < TENANTS; i++ {
	// 	t := fmt.Sprintf("t%d", i)
	// 	tenants = append(tenants, t)
	// 	//counts[t] = 0
	// }
	memory := 128 // KB
	w := memory * 1024 * 8 / ecm.DEFAULTS.CounterSize
	conf := &ecm.DEFAULTS
	conf.WindowSize = uint32(WindowSize)
	sketch, err := ecm.NewSketch(conf, w/4, 4)
	if err != nil {
		fmt.Println(err)
		return
	}

	var t int64

	for t = 0; t < Requests/2; t++ {
		ten := tn(rand.Intn(TENANTS))
		sketch.Insert(ten, 0)
		counts[ten]++
	}
	for t = Requests / 2; t < Requests-1; t++ {
		ten := tn(TENANTS - rand.Intn(25) - 1)
		sketch.Insert(ten, 0)
		counts[ten]++
	}
	fmt.Println("Done Inserting")
	for i := 0; i < TENANTS; i++ {
		real := counts[tn(i)]
		//real := 0
		//ecm_result := ecm.Query(tn(i), (Requests-1)/int64(WindowFactor/WindowSize))
		ecm_result := sketch.InsertAndQuery(tn(i), 0)
		fmt.Printf("%s, real: %d\t\tecm: %d\n", tn(i), real, ecm_result)
		//fmt.Printf("%s, real: \t\tecm: %d\n", tn(i), ecm_result)
	}
	fmt.Println("Done querying")

}
