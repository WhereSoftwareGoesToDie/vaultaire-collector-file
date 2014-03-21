package main

// Config defines the configuration file format for
// vaultaire-collector-file.
type Config struct {
	General struct {
		// Writes to execute in parallel.
		Parallelism int
		// Writes to perform as one operation (where
		// applicable).
		BatchSize      int
		StorageBackend string
		LogFile        string
		LogLevel       string
	}
	Vaultaire struct {
		Broker        string
		BatchPeriod   float64
		Origin        string
		MarquiseDebug bool
		TelemetryEndpoint string
	}
	File struct {
		DataFrameFile string
	}
}
