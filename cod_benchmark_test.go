package lshensemble

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	benchmarkSeed = 42
	numQueries    = 1000
	minDomainSize = 10
	minQuerySize  = 10
	maxQuerySize  = 100
)

var (
	thresholds = []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}
	numHash    = []int{16, 32, 64, 128}
)

// Running this function requires a `_cod_domains` directory
// in the current directory.
// The `_code_domains` directory should contain domains files,
// which are line-separated files.
func Benchmark_CanadianOpenData(b *testing.B) {
	// Read raw domains
	start := time.Now()
	rawDomains := make([]rawDomain, 0)
	var count int
	fmt.Println()
	for domain := range readDomains("_cod_domains") {
		// Ignore domaisn with less than 10 values
		if len(domain.values) < minDomainSize {
			continue
		}
		rawDomains = append(rawDomains, domain)
		count++
		fmt.Printf("\rRead %d domains", count)
	}
	fmt.Println()
	log.Printf("Read %d domains in %s", len(rawDomains),
		time.Now().Sub(start).String())

	// Select queries
	//numQuery := int(fracQuery * float64(len(rawDomains)))
	queries := make([]rawDomain, 0, numQueries)
	i := 0
	for _, domain := range rawDomains {
		if i >= numQueries {
			break
		}
		if len(domain.values) >= minQuerySize && len(domain.values) <= maxQuerySize {
			queries = append(queries, domain)
			i++
		}
	}
	if len(queries) < numQueries {
		msg := fmt.Sprintf("Not enough queries, found %d", len(queries))
		panic(msg)
	}
	log.Printf("Selected %d queries", len(queries))
	// Run benchmark
	for _, numH := range numHash {
		precision := []float64{}
		recall := []float64{}
		f1 := []float64{}
		MinHashDomainTime := []float64{}
		MinHashDomainSpace := []float64{}
		MinHashQueryTime := []float64{}
		MinHashQuerySpace := []float64{}
		BuildIndexTime := []float64{}
		BuildIndexSpace := []float64{}
		QueryIndexTime := []float64{}
		QueryIndexSpace := []float64{}

		for _, threshold := range thresholds {
			log.Printf("Canadian Open Data benchmark with %d Hash permutations and threshold = %.2f", numH, threshold)
			linearscanOutput := fmt.Sprintf("_cod_linearscan_numHash_%d_threshold_%.2f", numH, threshold)
			lshensembleOutput := fmt.Sprintf("_cod_lshensemble_numHash_%d_threshold_%.2f", numH, threshold)
			accuracyOutput := fmt.Sprintf("_cod_accuracy_numHash_%d_threshold_%.2f", numH, threshold)
			benchmarkLinearscan(rawDomains, queries, threshold, linearscanOutput)
			t, s := benchmarkLshEnsemble(rawDomains, queries, threshold, numH, lshensembleOutput)
			p, r, f := benchmarkAccuracy(linearscanOutput, lshensembleOutput, accuracyOutput)
			precision = append(precision, p)
			recall = append(recall, r)
			f1 = append(f1, f)
			MinHashDomainTime = append(MinHashDomainTime, t[0])
			MinHashDomainSpace = append(MinHashDomainSpace, s[0])
			MinHashQueryTime = append(MinHashQueryTime, t[1])
			MinHashQuerySpace = append(MinHashQuerySpace, s[1])
			BuildIndexTime = append(BuildIndexTime, t[2])
			BuildIndexSpace = append(BuildIndexSpace, s[2])
			QueryIndexTime = append(QueryIndexTime, t[3])
			QueryIndexSpace = append(QueryIndexSpace, s[3])
			runtime.GC()
			os.Remove(linearscanOutput)
			os.Remove(lshensembleOutput)
		}
		// Output results
		accuracy_filename := fmt.Sprintf("_cod_accuracy_mm3_32_%d_small.csv", numH)
		file, err := os.Create(accuracy_filename)
		if err != nil {
			panic(err)
		}
		out := csv.NewWriter(file)
		out.Write([]string{"Precision", "Recall", "F1"})
		for i := 0; i < len(thresholds); i++ {
			log.Printf("Precision: %.4f, Recall: %.4f, F1: %.4f", precision[i], recall[i], f1[i])
			line := []string{
				strconv.FormatFloat(precision[i], 'f', 4, 64),
				strconv.FormatFloat(recall[i], 'f', 4, 64),
				strconv.FormatFloat(f1[i], 'f', 4, 64),
			}
			log.Printf(line[0] + line[1] + line[2])
			out.Write(line)
		}
		out.Flush()
		file.Close()
		log.Printf("Accuracy report output to %s", accuracy_filename)

		performance_filename := fmt.Sprintf("_cod_performance_mm3_32_%d_small.csv", numH)
		file, err = os.Create(performance_filename)
		if err != nil {
			panic(err)
		}
		out = csv.NewWriter(file)
		out.Write([]string{"MinHashDomain", "MinHashQuery", "LSHBuild", "LSHQuery"})
		line := []string{
			strconv.FormatFloat(mean(MinHashDomainTime), 'f', 4, 64),
			strconv.FormatFloat(mean(MinHashQueryTime), 'f', 4, 64),
			strconv.FormatFloat(mean(BuildIndexTime), 'f', 4, 64),
			strconv.FormatFloat(mean(QueryIndexTime), 'f', 4, 64),
		}
		out.Write(line)
		line = []string{
			strconv.FormatFloat(mean(MinHashDomainSpace), 'f', 4, 64),
			strconv.FormatFloat(mean(MinHashQuerySpace), 'f', 4, 64),
			strconv.FormatFloat(mean(BuildIndexSpace), 'f', 4, 64),
			strconv.FormatFloat(mean(QueryIndexSpace), 'f', 4, 64),
		}
		out.Write(line)
		out.Flush()
		file.Close()
		runtime.GC()
	}
}

type rawDomain struct {
	values map[string]bool
	key    string
}

type byKey []*rawDomain

func (ds byKey) Len() int           { return len(ds) }
func (ds byKey) Swap(i, j int)      { ds[i], ds[j] = ds[j], ds[i] }
func (ds byKey) Less(i, j int) bool { return ds[i].key < ds[j].key }

func int64ToFloat64(arr []int64) []float64 {
	result := make([]float64, len(arr))
	for i, v := range arr {
		result[i] = float64(v)
	}
	return result
}

func readDomains(dir string) chan rawDomain {
	out := make(chan rawDomain)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		msg := fmt.Sprintf("Error reading domain directory %s, does it exist?", dir)
		panic(msg)
	}
	go func() {
		for _, file := range files {
			key := file.Name()
			values := make(map[string]bool)
			domainFile, err := os.Open(filepath.Join(dir, key))
			if err != nil {
				panic(err)
			}
			scanner := bufio.NewScanner(domainFile)
			for scanner.Scan() {
				v := strings.ToLower(scanner.Text())
				values[v] = true
				err = scanner.Err()
				if err != nil {
					panic(err)
				}
			}
			domainFile.Close()
			out <- rawDomain{
				values: values,
				key:    key,
			}
		}
		close(out)
	}()
	return out
}

type queryResult struct {
	candidates []interface{}
	queryKey   interface{}
	duration   time.Duration
}

func outputQueryResults(results chan queryResult, outputFilename string) {
	f, err := os.Create(outputFilename)
	if err != nil {
		panic(err)
	}
	out := bufio.NewWriter(f)
	for result := range results {
		out.WriteString(result.queryKey.(string))
		out.WriteString("\t")
		out.WriteString(result.duration.String())
		out.WriteString("\t")
		for i, candidate := range result.candidates {
			out.WriteString(candidate.(string))
			if i < len(result.candidates)-1 {
				out.WriteString("\t")
			}
		}
		out.WriteString("\n")
	}
	out.Flush()
	f.Close()
}
