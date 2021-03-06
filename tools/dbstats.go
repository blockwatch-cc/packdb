// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// bolt-db status

package main

import (
	"flag"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"time"

	"blockwatch.cc/packdb/util"
)

var (
	verbose = flag.Bool("v", false, "be more verbose")
	p       = util.PrettyInt
)

func fail(err error) {
	fmt.Printf("Error: %v\n", err)
	os.Exit(1)
}

func failUsage(err error) {
	fmt.Printf("Error: %v\n", err)
	flag.PrintDefaults()
	os.Exit(1)
}

type summary struct {
	Name  string
	Keys  int
	Used  int
	Alloc int
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: dbstats [-v] path\n")
	}
	flag.Parse()
	if flag.NArg() < 1 {
		failUsage(fmt.Errorf("missing db"))
	}
	dbname := flag.Arg(0)
	start := time.Now()

	db, err := bolt.Open(dbname, 0666, &bolt.Options{
		Timeout:    time.Second, // open timeout when file is locked
		NoGrowSync: true,        // assuming Docker + XFS
		ReadOnly:   true,
	})
	if err != nil {
		fail(err)
	}

	total := summary{Name: "Total"}
	perBucket := make([]summary, 0)

	if *verbose {
		// print db statistics
		fmt.Printf("Database Statistics\n")
		fmt.Printf("----------------------------------------------\n")
		dbstats := db.Stats()
		// Freelist stats
		fmt.Printf("FreePageN:         %10s  (total number of free pages on the freelist)\n", p(dbstats.FreePageN))
		fmt.Printf("PendingPageN:      %10s  (total number of pending pages on the freelist)\n", p(dbstats.PendingPageN))
		fmt.Printf("FreeAlloc:         %10s  (total bytes allocated in free pages)\n", p(dbstats.FreeAlloc))
		fmt.Printf("FreelistInuse:     %10s  (total bytes used by the freelist)\n", p(dbstats.FreelistInuse))
		// Transaction stats
		fmt.Printf("TxN:               %10s  (total number of started read transactions)\n", p(dbstats.TxN))
		fmt.Printf("OpenTxN:           %10s  (number of currently open read transactions)\n", p(dbstats.OpenTxN))
		// Ongong TX stats
		// Page statistics.
		fmt.Printf("PageCount:         %10s  (number of page allocations)\n", p(dbstats.TxStats.PageCount))
		fmt.Printf("PageAlloc:         %10s  (total bytes allocated)\n", p(dbstats.TxStats.PageAlloc))
		// Cursor statistics.
		fmt.Printf("CursorCount:       %10s  (number of cursors created)\n", p(dbstats.TxStats.CursorCount))
		// Node statistics
		fmt.Printf("NodeCount:         %10s  (number of node allocations)\n", p(dbstats.TxStats.NodeCount))
		fmt.Printf("NodeDeref:         %10s  (number of node dereferences)\n", p(dbstats.TxStats.NodeDeref))
		// Rebalance statistics.
		fmt.Printf("Rebalance:         %10s  (number of node rebalances)\n", p(dbstats.TxStats.Rebalance))
		fmt.Printf("RebalanceTime:     %11s (total time spent rebalancing)\n", dbstats.TxStats.RebalanceTime)
		// Split/Spill statistics.
		fmt.Printf("Split:             %10s  (number of nodes split)\n", p(dbstats.TxStats.Split))
		fmt.Printf("Spill:             %10s  (number of nodes spilled)\n", p(dbstats.TxStats.Spill))
		fmt.Printf("SpillTime:         %11s (total time spent spilling)\n", dbstats.TxStats.SpillTime)
		// Write statistics.
		fmt.Printf("Write:             %10s  (number of writes performed)\n", p(dbstats.TxStats.Write))
		fmt.Printf("WriteTime:         %11s (total time spent writing to disk)\n", dbstats.TxStats.WriteTime)
	}

	err = db.View(func(tx *bolt.Tx) error {
		// print bucket statstics
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			stats := b.Stats()
			total.Keys += stats.KeyN
			total.Used += stats.LeafInuse + stats.BranchInuse + stats.InlineBucketInuse
			total.Alloc += stats.LeafAlloc + stats.BranchAlloc
			perBucket = append(perBucket, summary{
				Name:  string(name),
				Keys:  stats.KeyN,
				Used:  stats.LeafInuse + stats.BranchInuse + stats.InlineBucketInuse,
				Alloc: stats.LeafAlloc + stats.BranchAlloc,
			})

			if *verbose {
				fmt.Printf("\nBucket Statistics: %s\n", string(name))
				fmt.Printf("----------------------------------------------\n")

				// Page count statistics.
				fmt.Printf("BranchPageN:       %10s  (number of logical branch pages)\n", p(stats.BranchPageN))
				fmt.Printf("BranchOverflowN:   %10s  (number of physical branch overflow pages)\n", p(stats.BranchOverflowN))
				fmt.Printf("LeafPageN:         %10s  (number of logical leaf pages)\n", p(stats.LeafPageN))
				fmt.Printf("LeafOverflowN:     %10s  (number of physical leaf overflow pages)\n", p(stats.LeafOverflowN))

				// Tree statistics.
				fmt.Printf("KeyN:              %10s  (number of keys/value pairs)\n", p(stats.KeyN))
				fmt.Printf("Depth:             %10s  (number of levels in B+tree)\n", p(stats.Depth))

				// Page size utilization.
				fmt.Printf("BranchAlloc:       %10s  (bytes allocated for physical branch pages)\n", p(stats.BranchAlloc))
				fmt.Printf("BranchInuse:       %10s  (bytes actually used for branch data)\n", p(stats.BranchInuse))
				fmt.Printf("LeafAlloc:         %10s  (bytes allocated for physical leaf pages)\n", p(stats.LeafAlloc))
				fmt.Printf("LeafInuse:         %10s  (bytes actually used for leaf data)\n", p(stats.LeafInuse))

				// Bucket statistics
				fmt.Printf("BucketN:           %10s  (total number of buckets including the top bucket)\n", p(stats.BucketN))
				fmt.Printf("InlineBucketN:     %10s  (total number on inlined buckets)\n", p(stats.InlineBucketN))
				fmt.Printf("InlineBucketInuse: %10s  (bytes used for inlined buckets (also accounted for in LeafInuse))\n", p(stats.InlineBucketInuse))
			}
			return nil
		})
	})
	if err != nil {
		fail(err)
	}

	if *verbose {
		fmt.Printf("\n")
	}

	// find longest name
	var lName int
	for _, v := range perBucket {
		lName = util.Max(lName, len(v.Name))
	}

	// write percentages as summary
	fmt.Printf("%[1]*s      %15s           %15s           %15s\n",
		-lName, "Name", "Keys (%)", "Alloc Bytes (%)", "Used Bytes (%)")
	fmt.Printf("%[1]*s  %15s (%6.2f)  %15s (%6.2f)  %15s (%6.2f)\n",
		-lName, total.Name, p(total.Keys), 100.0, p(total.Alloc), 100.0, p(total.Used), 100.0)
	for _, v := range perBucket {
		fmt.Printf("%[1]*s  %15s (%6.2f)  %15s (%6.2f)  %15s (%6.2f)\n",
			-lName, v.Name, p(v.Keys), pct(v.Keys, total.Keys), p(v.Alloc), pct(v.Alloc, total.Alloc), p(v.Used), pct(v.Used, total.Used))
	}

	if *verbose {
		fmt.Printf("\nDone in %s\n", time.Since(start))
	}
}

func pct(x, n int) float64 {
	return float64(x) / float64(n) * 100
}
