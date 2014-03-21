package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/anchor/bletchley/dataframe"
	"github.com/anchor/bletchley/framestore"
	"github.com/anchor/bletchley/framestore/vaultaire"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"time"
)

const (
	Version = "1.0.0"
)

const (
	DefaultParallelism = 1
	DefaultBatchSize   = 1000
	Version            = "0.1"
)

func WriteFrame(semaphore, resultChan chan int, writer framestore.DataFrameWriter, frame *dataframe.DataFrame) {
	// Block until we're ready to write
	<-semaphore
	err := writer.WriteFrame(frame)
	semaphore <- 1
	if err == nil {
		resultChan <- 1
	} else {
		log.Println("Write failed: ", err)
		resultChan <- 0
	}
}

func main() {
	rcfile := flag.String("cfg", "/etc/bletchley/framestore.gcfg", "Path to configuration file. This file should be in gcfg[0] format. [0] https://code.google.com/p/gcfg/")
	version := flag.Bool("version", false, "Print version number and then exit.")

	flag.Usage = func() {
		helpMessage := "bletchley_analyse will export DataFrames to a storage backend for analysis.\n\n" +
			"If no DataFrame files are passed on the command-line, it will read from stdin.\n\n" +
			fmt.Sprintf("Usage: %s [options] [datafile0 [datafile1 ... ] ]\n\n", os.Args[0]) +
			"Options:\n\n"
		fmt.Fprintf(os.Stderr, helpMessage)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *version {
		fmt.Println(Version)
		os.Exit(0)
	}

	cfgPath := *rcfile
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	if !filepath.IsAbs(cfgPath) {
		cfgPath = filepath.Join(usr.HomeDir, cfgPath)
	}
	cfg, err := InitializeConfig(cfgPath)
	if err != nil {
		log.Fatalf("Cannot initialize configuration "+
			"from config file at %v: %v", cfgPath, err)
	}

	parallelism := DefaultParallelism
	if cfg.General.Parallelism > 0 {
		parallelism = cfg.General.Parallelism
	}

	batchSize := DefaultBatchSize
	if cfg.General.BatchSize > 0 {
		batchSize = cfg.General.BatchSize
	}

	var writer framestore.DataFrameWriter

	if cfg.General.StorageBackend == "file" {
		writer, err = framestore.NewFileWriter(cfg.File.DataFrameFile)
	} else if cfg.General.StorageBackend == "vaultaire" {
		writer, err = vaultaire.NewVaultaireWriter(cfg.Vaultaire.Broker, cfg.Vaultaire.BatchPeriod, cfg.Vaultaire.Origin, "", cfg.Vaultaire.MarquiseDebug)
	} else {
		Log.Infof("No backend specified. Exiting.")
		os.Exit(0)
	}
	if err != nil {
		Log.Fatalf("Couldn't initialize writer: ", err)
	}

	defer writer.Shutdown()

	writerThreads := 0
	framesWritten := 0
	writeStart := time.Now()

	Log.Debugf("Starting writes at %v", writeStart)

	semaphore := make(chan int, parallelism)
	resultChannel := make(chan int, 0)

	for i := 0; i < parallelism; i++ {
		semaphore <- 1
	}
	dataFiles := make([]*os.File, 0)
	if len(os.Args) <= 1 {
		dataFiles = append(dataFiles, os.Stdin)
	} else {
		for _, path := range os.Args[1:] {
			fi, err := os.Open(path)
			if err != nil {
				Log.Fatalf(fmt.Sprintf("Couldn't open file %v: %v", err))
			}
			dataFiles = append(dataFiles, fi)
		}
	}
	for _, file := range dataFiles {
		var buf bytes.Buffer
		_, err = buf.ReadFrom(file)
		if err != nil {
			fmt.Println(err)
		}
		burst, err := dataframe.UnmarshalDataBurst(buf.Bytes())
		if err != nil {
			fmt.Println(err)
		}
		for _, frame := range burst.Frames {
			go WriteFrame(semaphore, resultChannel, writer, frame)
			writerThreads += 1
			// We want to kick off the channel-reader at regular
			// intervals in order to avoid exhausting our stack
			// space with waiting goroutines.
			if writerThreads%batchSize == 0 {
				for i := 0; i < writerThreads; i++ {
					framesWritten += <-resultChannel
					writerThreads--
				}
			}
		}
		for i := 0; i < writerThreads; i++ {
			framesWritten += <-resultChannel
		}
		if err != io.EOF {
			fmt.Println(err)
		}
	}
	writeEnd := time.Now()
	delta := writeEnd.Sub(writeStart)
	Log.Debugf("\nWrote %v frames in %v seconds at %v frames/second.\n", framesWritten, delta.Seconds(), float64(framesWritten)/delta.Seconds())
}
