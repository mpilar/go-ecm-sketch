package ecm

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
)

type ECMSketchConfig struct {
	MaxHashNum      int
	FilterSize      int
	CounterSize     int
	LowCounterSize  int
	HighCounterSize int
	WindowSize      uint32
	BucketCount     int
}

var (
	DEFAULTS = ECMSketchConfig{
		MaxHashNum:  20,
		FilterSize:  32,
		CounterSize: 32,
		WindowSize:  600,
		BucketCount: 100, //TODO: Analyze if 100 is necessary
	}

	SEED_PRIMES = []uint16{
		20177, 20183, 20201, 20219, 20231, 20233, 20249, 20261, 20269, 20287,
		20297, 20323, 20327, 20333, 20341, 20347, 20353, 20357, 20359, 20369,
		20389, 20393, 20399, 20407, 20411, 20431, 20441, 20443, 20477, 20479,
		20483, 20507, 20509, 20521, 20533, 20543, 20549, 20551, 20563, 20593,
		20599, 20611, 20627, 20639, 20641, 20663, 20681, 20693, 20707, 20717,
	}
	MAX_STAMP = math.MaxUint32
)

type ecmBucket struct {
	exponent int
	end      uint32
	start    uint32
}

type counter struct {
	bucket []*ecmBucket
	number int32
}

type ECMSketch struct {
	config           *ECMSketchConfig
	counter          [][]*counter
	hashSeeds        [][]byte
	hashes           []hash.Hash32
	index            []uint32 //TODO: index should be it's own struct in order to modularize "fillIndexes()" and allow for safe and unsafe versions
	width            int
	depth            int
	maxCnt           int
	counterIndexSize int
}

func NewSketch(config *ECMSketchConfig, width int, depth int) (*ECMSketch, error) {
	s := &ECMSketch{}
	s.counterIndexSize = 20
	s.width = width
	s.depth = depth
	s.config = config

	if s.depth > len(SEED_PRIMES) {
		return nil, fmt.Errorf("depth argument cannot be larger than the amount of available SEED_PRIMES (%d)", len(SEED_PRIMES))
	}

	s.hashSeeds = make([][]byte, s.depth)
	for i := 0; i < s.depth; i++ {
		token := make([]byte, 2)
		binary.LittleEndian.PutUint16(token, SEED_PRIMES[i])
		s.hashSeeds[i] = token
	}

	s.index = make([]uint32, s.depth)
	s.counter = make([][]*counter, s.depth)
	s.hashes = make([]hash.Hash32, s.depth)
	for i := 0; i < s.depth; i++ {
		s.counter[i] = make([]*counter, s.width)
		for j := 0; j < s.width; j++ {
			s.counter[i][j] = new(counter)
			s.counter[i][j].number = -1
			s.counter[i][j].bucket = make([]*ecmBucket, s.config.BucketCount)
			for x := 0; x < s.config.BucketCount; x++ {
				buck := new(ecmBucket)
				buck.exponent = -1
				s.counter[i][j].bucket[x] = buck
			}
		}
		// Create and seed the hashes
		s.hashes[i] = fnv.New32()
		s.hashes[i].Write(s.hashSeeds[i])
	}

	s.maxCnt = math.MaxInt32

	return s, nil
}

func (s *ECMSketch) expireBucket(i int, j uint32, t int64) {
	counter := s.counter[i][j]
	z := counter.number - 1
	stamp := t % int64(MAX_STAMP)
	// Default counter.number is -1, so from line above this means that this counter is uninitialized
	if z != -2 {
		for q := z; q >= 0; q-- {
			if int(counter.bucket[q].end)-1 <= int(stamp)-int(s.config.WindowSize) {
				counter.bucket[q].exponent = -1
				counter.bucket[q].start = 0
				counter.bucket[q].end = 0
				counter.number--
			} else {
				break
			}
		}
	}
}

func (s *ECMSketch) insertBucket(i int, j uint32, t int64) {
	counter := s.counter[i][j]
	z := counter.number
	p := int32(-1)
	value := 0
	first := int32(0)
	stamp := uint32(t % int64(MAX_STAMP))
	if counter.number == -1 {
		counter.bucket[0].exponent = 0
		counter.bucket[0].start = stamp
		counter.bucket[0].end = stamp
		counter.number = 1
	} else {
		for p < z {
			if (counter.bucket[p+2].exponent == value) && (p == -1) {
				counter.bucket[p+2].exponent++
				counter.bucket[p+2].end = counter.bucket[p+1].end
				counter.bucket[p+1].start = counter.bucket[p+2].end
				counter.bucket[p+1].end = stamp
				p = p + 2
				value = counter.bucket[p].exponent
			} else if (counter.bucket[p+2].exponent == value) && (p != -1) {
				counter.bucket[p+2].exponent++
				counter.bucket[p+2].end = counter.bucket[p+1].end
				counter.bucket[p+1].start = counter.bucket[p+2].end
				for q := p + 1; q > first; q-- {
					counter.bucket[q].exponent = counter.bucket[q-1].exponent
					counter.bucket[q].start = counter.bucket[q-1].start
					counter.bucket[q].end = counter.bucket[q-1].end
				}
				first++
				counter.number--
				p = p + 2
				value = counter.bucket[p].exponent
			} else if (counter.bucket[p+2].exponent != value) && (p != -1) {
				break
			} else {
				for q := z; q > 0; q-- {
					counter.bucket[q].exponent = counter.bucket[q-1].exponent
					counter.bucket[q].start = counter.bucket[q-1].start
					counter.bucket[q].end = counter.bucket[q-1].end
				}
				counter.number++
				counter.bucket[0].exponent = 0
				counter.bucket[0].start = counter.bucket[1].end
				counter.bucket[0].end = stamp
				break
			}
		}
		qq := int32(0)
		if first != 0 {
			for q := first; q < first+counter.number; q++ {
				counter.bucket[qq].exponent = counter.bucket[q].exponent
				counter.bucket[qq].start = counter.bucket[q].start
				counter.bucket[qq].end = counter.bucket[q].end
				qq++
			}
			for q := qq; q < qq+first; q++ {
				counter.bucket[q].exponent = -1
				counter.bucket[q].start = 0
				counter.bucket[q].end = 0
			}
		}
	}
}

func (s *ECMSketch) Insert(str string, stamp int64) {
	s.fillIndex(str)
	for i := 0; i < s.depth; i++ {
		s.expireBucket(i, s.index[i], stamp)
		s.insertBucket(i, s.index[i], stamp)
	}
}

func (s *ECMSketch) bucketSum(i int, j uint32) int {
	counter := s.counter[i][j]
	z := counter.number
	if z == -1 {
		return 0
	}
	exp := 0
	count_bucket := 1
	count := 0
	q := int32(0)

	for q = 0; q < z-1; q++ {
		exp = counter.bucket[q].exponent
		for k := 0; k < exp; k++ {
			count_bucket = 2 * count_bucket
		}
		count = count + count_bucket
		count_bucket = 1
	}
	for k := 0; k < counter.bucket[q].exponent; k++ {
		count_bucket = 2 * count_bucket
	}
	count = count + count_bucket/2

	return count
}

func (s *ECMSketch) fillIndex(str string) {
	st := []byte(str)
	for i := 0; i < s.depth; i++ {
		s.hashes[i].Write(st)
		s.index[i] = s.hashes[i].Sum32() % uint32(s.width)
		s.hashes[i].Reset()
		s.hashes[i].Write(s.hashSeeds[i])
	}
}

func (s *ECMSketch) Query(str string, stamp int64) int {
	min_value := s.maxCnt
	temp := 0
	s.fillIndex(str)
	for i := 0; i < s.depth; i++ {
		s.expireBucket(i, s.index[i], stamp)
		temp = s.bucketSum(i, s.index[i])
		if temp < min_value {
			min_value = temp
		}
	}
	return min_value
}

func (s *ECMSketch) InsertAndQuery(str string, stamp int64) int {
	min_value := s.maxCnt
	temp := 0
	s.fillIndex(str)
	for i := 0; i < s.depth; i++ {
		s.expireBucket(i, s.index[i], stamp)
		s.insertBucket(i, s.index[i], stamp)
		temp = s.bucketSum(i, s.index[i])
		if temp < min_value {
			min_value = temp
		}
	}
	return min_value
}
