//go:build sasl
// +build sasl

package mgo

import (
	"github.com/juju/mgo/v3/internal/sasl"
)

func saslNew(cred Credential, host string) (saslStepper, error) {
	return sasl.New(cred.Username, cred.Password, cred.Mechanism, cred.Service, host)
}
