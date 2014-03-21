package main

import (
	"code.google.com/p/gcfg"
	"fmt"
	"github.com/anchor/picolog"
	"os"
)

// Global logger
var Log picolog.Logger

// newLogger takes a populated Config object and a subpackage
// name and returns a configured picolog.Logger with framestore
// defaults.
func newLogger(cfg Config, subpackage string) (*picolog.Logger, error) {
	logLevel, err := picolog.ParseLogLevel(cfg.General.LogLevel)
	if err != nil {
		return nil, err
	}
	fo, err := os.OpenFile(cfg.General.LogFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	packageTag := fmt.Sprintf("vaultaire/%v", subpackage)
	logger := picolog.NewLogger(logLevel, packageTag, fo)
	return logger, nil
}

// InitializeLog takes a (populated) Config object and a subpackage name
// and configures the global framestore logger.
func InitializeLog(cfg Config, subpackage string) error {
	var err error
	logger, err := newLogger(cfg, subpackage)
	if err != nil {
		return err
	}
	Log = *logger
	return nil
}

// InitializeFramestoreConfig takes a configuration file path and a
// subpackage name.
//
// The configuration file must be in gcfg format and conform to the
// example given at doc/vaultaire-nagios.gcfg in the repository root.
func InitializeConfig(cfgPath string) (Config, error) {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, cfgPath)
	if err != nil {
		return cfg, err
	}
	err = InitializeLog(cfg, "nagios")
	if err != nil {
		return cfg, err
	}
	return cfg, nil
}
