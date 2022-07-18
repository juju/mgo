// Copyright 2022 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package testing_test

import (
	stdtesting "testing"

	"github.com/juju/mgo/v2/testing"
)

func Test(t *stdtesting.T) {
	testing.MgoTestPackage(t, nil)
}
