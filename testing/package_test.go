// Copyright 2022 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package testing_test

import (
	stdtesting "testing"

	"github.com/juju/mgo/v3/testing"
)

func Test(t *stdtesting.T) {
	testing.MgoTestPackage(t, nil)
}
