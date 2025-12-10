// mgo - MongoDB driver for Go
//
// Copyright (c) 2010-2012 - Gustavo Niemeyer <gustavo@niemeyer.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package mgo

import (
	"net"
	"strconv"
	"sync"
)

var stats Stats

func SetStats(enabled bool) {
	stats.reset(enabled)
}

func GetStats() Stats {
	stats.mu.RLock()
	defer stats.mu.RUnlock()
	return stats
}

// GetStatsForPort returns the stats for a specific server port, useful in tests.
func GetStatsForPort(port int) StatsByPort {
	stats.mu.RLock()
	defer stats.mu.RUnlock()
	s, _ := stats.ByPort[port]
	if s != nil {
		return *s
	}
	return StatsByPort{}
}

func ResetStats() {
	// If we call ResetStats we assume you want to use stats, so we enable
	// them.
	debug("Resetting stats")
	stats.reset(true)
}

type Stats struct {
	mu      sync.RWMutex
	enabled bool

	ByPort map[int]*StatsByPort

	Clusters     int
	MasterConns  int
	SlaveConns   int
	SentOps      int
	ReceivedOps  int
	ReceivedDocs int
	SocketsAlive int
	SocketsInUse int
	SocketRefs   int
}

type StatsByPort struct {
	Clusters     int
	MasterConns  int
	SlaveConns   int
	SentOps      int
	ReceivedOps  int
	ReceivedDocs int
	SocketsAlive int
	SocketsInUse int
	SocketRefs   int
}

func (stats *Stats) lockedStatsByPort(port int) *StatsByPort {
	if stats.ByPort == nil {
		stats.ByPort = make(map[int]*StatsByPort)
	}
	s, ok := stats.ByPort[port]
	if !ok {
		s = &StatsByPort{}
		stats.ByPort[port] = s
	}
	return s
}

func (stats *Stats) reset(enabled bool) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	for _, sbp := range stats.ByPort {
		sbp.MasterConns = 0
		sbp.SlaveConns = 0
		sbp.SentOps = 0
		sbp.ReceivedOps = 0
		sbp.ReceivedDocs = 0
	}

	stats.MasterConns = 0
	stats.SlaveConns = 0
	stats.SentOps = 0
	stats.ReceivedOps = 0
	stats.ReceivedDocs = 0

	if !enabled {
		// These are absolute values so we don't reset them unless we are
		// disabling stats altogether.
		stats.Clusters = 0
		stats.SocketsInUse = 0
		stats.SocketsAlive = 0
		stats.SocketRefs = 0

		stats.ByPort = nil
	}
}

func (stats *Stats) cluster(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).Clusters += delta
	stats.Clusters += delta
	stats.mu.Unlock()
}

func (stats *Stats) conn(delta int, master bool, port int) {
	stats.mu.Lock()
	if master {
		stats.lockedStatsByPort(port).MasterConns += delta
		stats.MasterConns += delta
	} else {
		stats.lockedStatsByPort(port).SlaveConns += delta
		stats.SlaveConns += delta
	}
	stats.mu.Unlock()
}

func (stats *Stats) sentOps(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).SentOps += delta
	stats.SentOps += delta
	stats.mu.Unlock()
}

func (stats *Stats) receivedOps(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).ReceivedOps += delta
	stats.ReceivedOps += delta
	stats.mu.Unlock()
}

func (stats *Stats) receivedDocs(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).ReceivedDocs += delta
	stats.ReceivedDocs += delta
	stats.mu.Unlock()
}

func (stats *Stats) socketsInUse(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).SocketsInUse += delta
	stats.SocketsInUse += delta
	stats.mu.Unlock()
}

func (stats *Stats) socketsAlive(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).SocketsAlive += delta
	stats.SocketsAlive += delta
	stats.mu.Unlock()
}

func (stats *Stats) socketRefs(delta int, port int) {
	stats.mu.Lock()
	stats.lockedStatsByPort(port).SocketRefs += delta
	stats.SocketRefs += delta
	stats.mu.Unlock()
}

func statsPort(addr string) int {
	_, strPort, _ := net.SplitHostPort(addr)
	intPort, _ := strconv.Atoi(strPort)
	return intPort
}
