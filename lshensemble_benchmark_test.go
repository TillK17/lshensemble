package lshensemble

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"time"
)

const (
	numHash = 256
	numPart = 32
	maxK    = 4
	// useOptimalPartitions = true
	useOptimalPartitions = false
)

func benchmarkLshEnsemble(rawDomains []rawDomain, rawQueries []rawDomain,
	threshold float64, outputFilename string) {
	numHash := 256
	numPart := 32
	maxK := 4

	// Minhash domains
	mem := readMemStats()
	start := time.Now()
	domainRecords := minhashDomains(rawDomains, numHash)
	minHashDomainTime := time.Now().Sub(start).String()
	minHashDomainSpace := readMemStats() - mem
	log.Printf("Minhash %d domains in %s", len(domainRecords),
		time.Now().Sub(start).String())

	// Minhash queries
	mem = readMemStats()
	start = time.Now()
	queries := minhashDomains(rawQueries, numHash)
	minHashQueryTime := time.Now().Sub(start).String()
	minHashQuerySpace := readMemStats() - mem
	log.Printf("Minhash %d query domains in %s", len(queries),
		time.Now().Sub(start).String())

	// Start main body of lsh ensemble
	// Indexing
	log.Print("Start building LSH Ensemble index")
	mem = readMemStats()
	start = time.Now()
	sort.Sort(BySize(domainRecords))
	var index *LshEnsemble
	if useOptimalPartitions {
		index, _ = BootstrapLshEnsemblePlusOptimal(numPart, numHash, maxK,
			func() <-chan *DomainRecord { return Recs2Chan(domainRecords) })
	} else {
		index, _ = BootstrapLshEnsemblePlusEquiDepth(numPart, numHash, maxK,
			len(domainRecords), Recs2Chan(domainRecords))
	}
	buildIndexTime := time.Now().Sub(start).String()
	buildIndexSpace := readMemStats() - mem
	log.Print("Finished building LSH Ensemble index")
	// Querying
	log.Printf("Start querying LSH Ensemble index with %d queries", len(queries))
	results := make(chan queryResult)
	mem = readMemStats()
	start = time.Now()
	go func() {
		for _, query := range queries {
			r, d := index.QueryTimed(query.Signature, query.Size, threshold)
			results <- queryResult{
				queryKey:   query.Key,
				duration:   d,
				candidates: r,
			}
		}
		close(results)
	}()
	queryIndexTime := time.Now().Sub(start).String()
	queryIndexSpace := readMemStats() - mem
	// Output results
	file, err := os.Create(fmt.Sprintf("performance_%f.csv", threshold))
	if err != nil {
		panic(err)
	}
	out := csv.NewWriter(file)
	out.Write([]string{"MinHashDomain", "MinHashQuery", "LSHBuild", "LSHQuery"})
	out.Write([]string{minHashDomainTime, minHashQueryTime, buildIndexTime, queryIndexTime})
	out.Write([]string{fmt.Sprintf("%d", minHashDomainSpace), fmt.Sprintf("%d", minHashQuerySpace), fmt.Sprintf("%d", buildIndexSpace), fmt.Sprintf("%d", queryIndexSpace)})
	out.Flush()
	file.Close()
	outputQueryResults(results, outputFilename)
	log.Printf("Finished querying LSH Ensemble index, output %s", outputFilename)
}

func minhashDomains(rawDomains []rawDomain, numHash int) []*DomainRecord {
	domainRecords := make([]*DomainRecord, 0)
	for _, domain := range rawDomains {
		mh := NewMinhash(benchmarkSeed, numHash)
		for v := range domain.values {
			mh.Push([]byte(v))
		}
		domainRecords = append(domainRecords, &DomainRecord{
			Key:       domain.key,
			Size:      len(domain.values),
			Signature: mh.Signature(),
		})
	}
	return domainRecords
}

func readMemStats() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc / 1024 / 1024
}
