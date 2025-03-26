package lshensemble

import (
	"hash/fnv"
	"log"
	"math"
	"runtime"
	"sort"
	"time"
)

func benchmarkKMV(rawDomains []rawDomain, rawQueries []rawDomain,
	threshold float64, k uint64, outputFilename string) ([]float64, []float64) {
	// KMV domains
	mem := readMemStats()
	start := time.Now()
	domainRecords := KMVDomains(rawDomains, k)
	KMVDomainTime := time.Now().Unix() - start.Unix()
	KMVDomainSpace := readMemStats() - mem
	log.Printf("KMV %d domains in %s", len(domainRecords),
		time.Now().Sub(start).String())

	// KMV queries
	mem = readMemStats()
	start = time.Now()
	queries := KMVDomains(rawQueries, k)
	KMVQueryTime := time.Now().Unix() - start.Unix()
	KMVQuerySpace := readMemStats() - mem
	log.Printf("KMV %d query domains in %s", len(queries),
		time.Now().Sub(start).String())

	// Start comparing queries
	log.Print("Start comparing queries")
	results := make(chan queryResult)
	mem = readMemStats()
	start = time.Now()

	for _, query := range queries {
		candidates := make([]interface{}, 0)
		start := time.Now()
		for _, domain := range domainRecords {
			// Compare the query with the domain
			u := union(domain.Signature, query.Signature)
			i := intersectinSize(domain.Signature, query.Signature)
			est := (i * float64(k-1)) / (float64(k) * u[k-1])
			if est >= threshold {
				candidates = append(candidates, domain.Key)
			}
		}
		duration := time.Now().Sub(start)
		results <- queryResult{
			queryKey:   query.Key,
			duration:   duration,
			candidates: candidates,
		}
	}
	close(results)
	QueryTime := time.Now().Unix() - start.Unix()
	QuerySpace := readMemStats() - mem
	log.Printf("Finished comparing queries")

	outputQueryResults(results, outputFilename)
	log.Printf("Finished comparing queries, output %s", outputFilename)
	return []float64{float64(KMVDomainTime), float64(KMVQueryTime), float64(QueryTime)}, []float64{float64(KMVDomainSpace), float64(KMVQuerySpace), float64(QuerySpace)}
}

func KMVDomains(rawDomains []rawDomain, k uint64) []*DomainRecord {
	domainRecords := make([]*DomainRecord, 0)
	for _, domain := range rawDomains {
		signature := []float64{}
		for value := range domain.values {
			v := stringToHash(value)
			kmv := GRM_Hash(v)
			signature = append(signature, kmv)
		}

		// Sort the signature
		sort.Float64s(signature)

		domainRecords = append(domainRecords, &DomainRecord{
			Key:       domain.key,
			Size:      len(domain.values),
			Signature: signature[:k],
		})
	}
	return domainRecords
}

func GRM_Hash(e uint64) (Hashvalue float64) {
	GR := (1 + math.Sqrt(5)) / 2
	ele := float64(e) + 1.0
	Hashvalue = ele*GR - math.Floor(ele*GR)
	return
}

func stringToHash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func intersectinSize(domain []float64, query []float64) float64 {
	i, j := 0, 0
	intersectionSize := 0
	for i < len(domain) && j < len(query) {
		if domain[i] == query[j] {
			intersectionSize++
			i++
			j++
		} else if domain[i] < query[j] {
			i++
		} else {
			j++
		}
	}
	return float64(intersectionSize)
}

func union(domain []float64, query []float64) []float64 {
	unionSet := make(map[float64]bool)
	for _, v := range domain {
		unionSet[v] = true
	}
	for _, v := range query {
		unionSet[v] = true
	}
	unionSlice := make([]float64, 0, len(unionSet))
	for v := range unionSet {
		unionSlice = append(unionSlice, v)
	}
	return unionSlice
}

func readMemStats() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}
