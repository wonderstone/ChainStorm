package tools

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
)

// NewLogger creates a new multilevelWriter loger with level
// and can be turned off and set output to os.Stdout or file
func NewLogger(pars map[string]interface{}) zerolog.Logger {
	// whether the logger is enabled
	enabled := pars["enabled"].(bool)
	// level of the logger
	level := pars["level"].(string)
	// output of the logger, if stdout or file path, or both with "," separated
	outputs := pars["output"].(string)

	var logger zerolog.Logger
	if !enabled {
		logger = zerolog.New(io.Discard).With().Timestamp().Logger()
	} else {
		// set the log level 
		lvl , err := zerolog.ParseLevel(level)
		if err != nil {
			panic(err)
		}
		zerolog.SetGlobalLevel(lvl)

		// set the output
		// if the output string has "," separated values, then it is a multiwriter
		outputVals :=strings.Split(outputs, ",")
		// if outputVals has more than one value, then it is a multiwriter
		if len(outputVals) > 1 {
			var writers []io.Writer
			for _, output := range outputVals {
				if output == "stdout" {
					writers = append(writers, zerolog.ConsoleWriter{Out: os.Stdout})
				} else {
					// check the output path has a directory,
					// if not, create the directory
					dir := filepath.Dir(output)
					if _, err := os.Stat(dir); os.IsNotExist(err) {
						os.MkdirAll(dir, os.ModePerm)
					}		

					file, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
					if err != nil {
						panic(err)
					}
					writers = append(writers, file)
				}
			}
			logger = zerolog.New(io.MultiWriter(writers...)).With().Timestamp().Logger()
		} else {
			// if outputVals has only one value, then it is a single writer
			output := outputVals[0]
			if output == "stdout" {
				logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
			} else {
				file, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					panic(err)
				}

				logger = zerolog.New(file).With().Timestamp().Logger()
			}
		}
	}
	
	return logger
}

