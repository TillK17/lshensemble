package lshensemble

import (
	"log"
	"runtime"
	"sort"
	"time"
)

const (
	numPart = 32
	maxK    = 4
	// useOptimalPartitions = true
	useOptimalPartitions = false
)

func benchmarkLshEnsemble(rawDomains []rawDomain, rawQueries []rawDomain,
	threshold float64, numHash int, outputFilename string) ([]float64, []float64) {
	numPart := 32
	maxK := 4
	// Minhash domains
	mem := readMemStats()
	start := time.Now()
	domainRecords := minhashDomains(rawDomains, numHash)
	minHashDomainTime := time.Now().Unix() - start.Unix()
	minHashDomainSpace := readMemStats() - mem
	log.Printf("Minhash %d domains in %s", len(domainRecords),
		time.Now().Sub(start).String())

	// Minhash queries
	mem = readMemStats()
	start = time.Now()
	queries := minhashDomains(rawQueries, numHash)
	minHashQueryTime := time.Now().Unix() - start.Unix()
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
	buildIndexTime := time.Now().Unix() - start.Unix()
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
	queryIndexTime := time.Now().Unix() - start.Unix()
	queryIndexSpace := readMemStats() - mem
	// Output results
	/*file, err := os.Create(fmt.Sprintf(outputFilename+"performance_%f.csv", threshold))
	if err != nil {
		panic(err)
	}
	out := csv.NewWriter(file)
	out.Write([]string{"MinHashDomain", "MinHashQuery", "LSHBuild", "LSHQuery"})
	out.Write([]string{fmt.Sprintf("%d", minHashDomainTime), fmt.Sprintf("%d", minHashQueryTime), fmt.Sprintf("%d", buildIndexTime), fmt.Sprintf("%d", queryIndexTime)})
	out.Write([]string{fmt.Sprintf("%d", minHashDomainSpace), fmt.Sprintf("%d", minHashQuerySpace), fmt.Sprintf("%d", buildIndexSpace), fmt.Sprintf("%d", queryIndexSpace)})
	out.Flush()
	file.Close()*/
	outputQueryResults(results, outputFilename)
	log.Printf("Finished querying LSH Ensemble index, output %s", outputFilename)
	return []float64{float64(minHashDomainTime), float64(minHashQueryTime), float64(buildIndexTime), float64(queryIndexTime)}, []float64{float64(minHashDomainSpace), float64(minHashQuerySpace), float64(buildIndexSpace), float64(queryIndexSpace)}
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
	return m.TotalAlloc
}
