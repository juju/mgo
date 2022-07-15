// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package checkers_test

import (
	gc "gopkg.in/check.v1"

	jc "github.com/juju/testing/checkers"
)

type Inner struct {
	First  string
	Second int             `json:",omitempty" yaml:",omitempty"`
	Third  map[string]bool `json:",omitempty" yaml:",omitempty"`
}

type Outer struct {
	First  float64
	Second []*Inner `json:"Last,omitempty" yaml:"last,omitempty"`
}
