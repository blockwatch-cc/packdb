// Copyright (c) 2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// drops and rebuilds table metadata

package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "path/filepath"
    "runtime/debug"
    "strings"
    "time"

    bolt "go.etcd.io/bbolt"

    "blockwatch.cc/packdb/pack"
    "blockwatch.cc/packdb/store"
    _ "blockwatch.cc/packdb/store/bolt"
    "blockwatch.cc/packdb/util"
    "github.com/echa/log"
)

var (
    flags   = flag.NewFlagSet("rebuild-metadata", flag.ContinueOnError)
    verbose bool
    vdebug  bool
    vtrace  bool
    gogc    int
    dbname  string
    tname   string
)

var (
    p        = util.PrettyInt64
    pi       = util.PrettyInt
    boltopts = &bolt.Options{
        Timeout:      time.Second, // open timeout when file is locked
        NoGrowSync:   true,        // assuming Docker + XFS
        NoSync:       true,        // skip fsync (DANGEROUS on crashes)
        FreelistType: bolt.FreelistMapType,
    }
)

func b(n int) string {
    return util.ByteSize(n).String()
}

func init() {
    flags.Usage = func() {}
    flags.BoolVar(&verbose, "v", false, "be verbose")
    flags.BoolVar(&vdebug, "vv", false, "debug mode")
    flags.BoolVar(&vtrace, "vvv", false, "trace mode")
    flags.IntVar(&gogc, "gogc", 20, "gc `percentage`")
    flags.StringVar(&dbname, "db", "", "database `filename`")
    flags.StringVar(&tname, "table", "", "table `name`")
}

func main() {
    if err := run(); err != nil {
        log.Error(err)
    }
}

func run() error {
    if err := flags.Parse(os.Args[1:]); err != nil {
        if err == flag.ErrHelp {
            fmt.Println("Pack table metadata rebuild")
            flags.PrintDefaults()
            return nil
        }
        return err
    }
    lvl := log.LevelInfo
    switch true {
    case vtrace:
        lvl = log.LevelTrace
    case vdebug:
        lvl = log.LevelDebug
    case verbose:
        lvl = log.LevelInfo
    }
    log.SetLevel(lvl)
    pack.UseLogger(log.Log)

    // set GC trigger
    if gogc <= 0 {
        gogc = 20
    }
    // Block and transaction processing can cause bursty allocations. This
    // limits the garbage collector from excessively overallocating during
    // bursts. This value was arrived at with the help of profiling live
    // usage.
    debug.SetGCPercent(gogc)

    if dbname == "" {
        dbname = flags.Arg(0)
    }

    if dbname == "" {
        return fmt.Errorf("Missing database.")
    }

    name := strings.TrimSuffix(filepath.Base(dbname), ".db")
    if tname == "" {
        tname = name
    }

    start := time.Now()

    // open database file
    db, err := pack.OpenDatabase(filepath.Dir(dbname), name, "*", boltopts)
    if err != nil {
        return fmt.Errorf("opening database: %v", err)
    }
    defer db.Close()
    log.Infof("Using database %s", db.Path())

    // check table
    table, err := db.Table(tname)
    if err != nil {
        return err
    }
    // make sure source table journals are flushed
    if err := table.Flush(context.Background()); err != nil {
        return err
    }
    stats := table.Stats()
    table.Close()

    log.Infof("Rebuilding metadata for %d rows / %d packs with statistics size %d bytes",
        stats[0].TupleCount, stats[0].PacksCount, stats[0].MetaSize)

    // Delete table metadata bucket
    log.Info("Dropping table statistics")
    err = db.Update(func(dbTx store.Tx) error {
        meta := dbTx.Bucket([]byte(tname + "_meta"))
        if meta == nil {
            return fmt.Errorf("missing table metdata bucket")
        }
        err := meta.DeleteBucket([]byte("_headers"))
        if !store.IsError(err, store.ErrBucketNotFound) {
            return err
        }
        return nil
    })
    if err != nil {
        return err
    }

    // Open table, this will automatically rebuild all metadata
    log.Info("Rebuilding table statistics...")
    table, err = db.Table(tname)
    if err != nil {
        return err
    }

    // Close table, this will automatically store the new metadata
    stats = table.Stats()
    log.Info("Storing table statistics...")
    err = table.Close()
    if err != nil {
        return err
    }

    log.Infof("Rebuild took %s, new statistics size %d bytes", time.Since(start), stats[0].MetaSize)
    return nil
}
