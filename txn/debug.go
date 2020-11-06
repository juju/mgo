package txn

import (
	"bytes"
	"fmt"
	"sort"
	"sync/atomic"
	"unsafe"

	"gopkg.in/mgo.v2/bson"
)

var (
	// To ensure goroutine safety, use the getter
	// and setter methods below to access these.
	logger       unsafe.Pointer
	debugEnabled uint32
)

type log_Logger interface {
	Output(calldepth int, s string) error
}

// Specify the *log.Logger object where log messages should be sent to.
func SetLogger(l log_Logger) {
	if l == nil {
		atomic.StorePointer(&logger, nil)
		return
	}
	atomic.StorePointer(&logger, unsafe.Pointer(&l))
}

func getLogger() log_Logger {
	if loggerPtr := (*log_Logger)(atomic.LoadPointer(&logger)); loggerPtr != nil {
		return *loggerPtr
	}
	return nil
}

// Enable the delivery of debug messages to the logger.  Only meaningful
// if a logger is also set.
func SetDebug(debug bool) {
	value := uint32(0)
	if debug {
		value = 1
	}
	atomic.StoreUint32(&debugEnabled, value)
}

func getDebug() bool {
	return atomic.LoadUint32(&debugEnabled) != 0
}

var ErrChaos = fmt.Errorf("interrupted by chaos")

var debugId uint32

func debugPrefix() string {
	d := atomic.AddUint32(&debugId, 1) - 1
	s := make([]byte, 0, 10)
	for i := uint(0); i < 8; i++ {
		s = append(s, "abcdefghijklmnop"[(d>>(4*i))&0xf])
		if d>>(4*(i+1)) == 0 {
			break
		}
	}
	s = append(s, ')', ' ')
	return string(s)
}

func logf(format string, args ...interface{}) {
	if logger := getLogger(); logger != nil {
		logger.Output(2, fmt.Sprintf(format, argsForLog(args)...))
	}
}

func debugf(format string, args ...interface{}) {
	if logger := getLogger(); logger != nil && getDebug() {
		logger.Output(2, fmt.Sprintf(format, argsForLog(args)...))
	}
}

func argsForLog(args []interface{}) []interface{} {
	for i, arg := range args {
		switch v := arg.(type) {
		case bson.ObjectId:
			args[i] = v.Hex()
		case []bson.ObjectId:
			lst := make([]string, len(v))
			for j, id := range v {
				lst[j] = id.Hex()
			}
			args[i] = lst
		case map[docKey][]bson.ObjectId:
			buf := &bytes.Buffer{}
			var dkeys docKeys
			for dkey := range v {
				dkeys = append(dkeys, dkey)
			}
			sort.Sort(dkeys)
			for i, dkey := range dkeys {
				if i > 0 {
					buf.WriteByte(' ')
				}
				buf.WriteString(fmt.Sprintf("%v: {", dkey))
				for j, id := range v[dkey] {
					if j > 0 {
						buf.WriteByte(' ')
					}
					buf.WriteString(id.Hex())
				}
				buf.WriteByte('}')
			}
			args[i] = buf.String()
		case map[docKey][]int64:
			buf := &bytes.Buffer{}
			var dkeys docKeys
			for dkey := range v {
				dkeys = append(dkeys, dkey)
			}
			sort.Sort(dkeys)
			for i, dkey := range dkeys {
				if i > 0 {
					buf.WriteByte(' ')
				}
				buf.WriteString(fmt.Sprintf("%v: %v", dkey, v[dkey]))
			}
			args[i] = buf.String()
		}
	}
	return args
}
