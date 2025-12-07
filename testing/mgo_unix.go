// Copyright 2014 Canonical Ltd.
// Copyright 2014 Cloudbase Solutions SRL
// Licensed under the LGPLv3, see LICENCE file for details.

//go:build !windows
// +build !windows

package testing

import "os"

// DestroyWithLog causes mongod to exit, cleans up its data directory,
// and captures the last N lines of mongod's log output.
func (inst *mgoServer) DestroyWithLog() {
	inst.killAndCleanup(os.Interrupt)
}
