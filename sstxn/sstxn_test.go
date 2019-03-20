// Copyright 2018 Canonical Ltd.

package sstxn_test

import (
	"flag"
	"fmt"
	"sync"
	"testing"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/dbtest"
	"gopkg.in/mgo.v2/sstxn"
	"gopkg.in/mgo.v2/txn"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

var fast = flag.Bool("fast", false, "Skip slow tests")

type S struct {
	server   dbtest.DBServer
	session  *mgo.Session
	db       *mgo.Database
	txnlog   *mgo.Collection
	accounts *mgo.Collection
	runner   *sstxn.Runner
}

var _ = Suite(&S{})

type M map[string]interface{}

type log_Logger interface {
	Output(calldepth int, s string) error
}

func (s *S) SetUpSuite(c *C) {
	// mongod running from juju-db.mongod cannot see /tmp
	path := c.MkDir()
	c.Logf("setting DB path to: %q", path)
	s.server.SetPath(path)
	s.server.EnableReplicaset("sstxn-test")
}

func (s *S) TearDownSuite(c *C) {
	s.server.Stop()
}

type testLogger struct {
	c *C
}

func (t *testLogger) Tracef(message string, args ...interface{}) {
	t.logf("TRACE", message, args...)
}
func (t *testLogger) Debugf(message string, args ...interface{}) {
	t.logf("DEBUG", message, args...)
}
func (t *testLogger) Infof(message string, args ...interface{}) {
	t.logf("INFO", message, args...)
}
func (t *testLogger) Warningf(message string, args ...interface{}) {
	t.logf("WARNING", message, args...)
}
func (t *testLogger) Errorf(message string, args ...interface{}) {
	t.logf("ERROR", message, args...)
}
func (t *testLogger) Criticalf(message string, args ...interface{}) {
	t.logf("CRITICAL", message, args...)
}
func (t *testLogger) logf(level, message string, args ...interface{}) {
	t.c.Output(3, level+" "+fmt.Sprintf(message, args...))
}

func (s *S) SetUpTest(c *C) {
	s.server.Wipe()

	// txn.SetChaos(txn.Chaos{})
	// txn.SetLogger(c)
	// txn.SetDebug(true)

	s.session = s.server.Session()
	build, err := s.session.BuildInfo()
	c.Assert(err, IsNil)
	if !build.VersionAtLeast(4, 0) {
		// Skip in SetUpTest means TearDownTest won't be run.
		s.session.Close()
		c.Skip("server side transactions only supported with Mongo 4.0")
	}
	s.db = s.session.DB("test")
	s.txnlog = s.db.C("txn.log")
	s.accounts = s.db.C("accounts")
	s.accounts.Create(&mgo.CollectionInfo{})
	s.runner = sstxn.NewRunner(s.db, &testLogger{c})
}

func (s *S) TearDownTest(c *C) {
	s.session.Close()
}

type Account struct {
	Id      int `bson:"_id"`
	Balance int
}

func (s *S) TestDocExists(c *C) {
	err := s.accounts.Insert(M{"_id": 0, "balance": 300})
	c.Assert(err, IsNil)

	exists := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocExists,
	}}
	missing := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocMissing,
	}}

	err = s.runner.Run(exists, "")
	c.Assert(err, IsNil)
	err = s.runner.Run(missing, "")
	c.Assert(err, Equals, txn.ErrAborted)

	err = s.accounts.RemoveId(0)
	c.Assert(err, IsNil)

	err = s.runner.Run(exists, "")
	c.Assert(err, Equals, txn.ErrAborted)
	err = s.runner.Run(missing, "")
	c.Assert(err, IsNil)
}

func (s *S) TestInsert(c *C) {
	err := s.accounts.Insert(M{"_id": 0, "balance": 300})
	c.Assert(err, IsNil)

	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Insert: M{"balance": 200},
	}}

	// NOTE: this differs from client-side transactions
	// With client txns, Insert of an existing doc was ignored. With
	// server side transactions, trying to insert a doc that already
	// exists aborts the entire transaction, so we no longer hide
	// that fact.
	err = s.runner.Run(ops, "")
	c.Assert(err, Equals, txn.ErrAborted)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)

	ops[0].Id = 1
	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	err = s.accounts.FindId(1).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)
}

func (s *S) TestInsertStructID(c *C) {
	type id struct {
		FirstName string
		LastName  string
	}
	ops := []txn.Op{{
		C:      "accounts",
		Id:     id{FirstName: "John", LastName: "Jones"},
		Assert: txn.DocMissing,
		Insert: M{"balance": 200},
	}, {
		C:      "accounts",
		Id:     id{FirstName: "Sally", LastName: "Smith"},
		Assert: txn.DocMissing,
		Insert: M{"balance": 800},
	}}

	err := s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	n, err := s.accounts.Find(nil).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)
}

func (s *S) TestRemove(c *C) {
	err := s.accounts.Insert(M{"_id": 0, "balance": 300})
	c.Assert(err, IsNil)

	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Remove: true,
	}}

	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	err = s.accounts.FindId(0).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// Removing a non-existing doc does not abort the transaction,
	// so we preserve the behavior.
	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)
}

func (s *S) TestUpdate(c *C) {
	var err error
	err = s.accounts.Insert(M{"_id": 0, "balance": 200})
	c.Assert(err, IsNil)
	err = s.accounts.Insert(M{"_id": 1, "balance": 200})
	c.Assert(err, IsNil)

	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Update: M{"$inc": M{"balance": 100}},
	}}

	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)

	err = s.accounts.FindId(1).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)
}

func (s *S) TestInsertUpdate(c *C) {
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Insert: M{"balance": 200},
	}, {
		C:      "accounts",
		Id:     0,
		Update: M{"$inc": M{"balance": 100}},
	}}

	err := s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)

	// Note: Differs from Client-side transactions.
	// Since client TXN treats insert as a no-op, then the insert+update
	// was causing the Update to happen, ignoring the invalid Insert.
	// We no longer treat Insert as a no-op, so we don't
	// run the Update.
	err = s.runner.Run(ops, "")
	c.Assert(err, Equals, txn.ErrAborted)

	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)
	// v- this is client-txn result
	// c.Assert(account.Balance, Equals, 400)
}

func (s *S) TestUpdateInsert(c *C) {
	c.Skip("client side transactions seems to have weird behavior here")
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Update: M{"$inc": M{"balance": 100}},
	}, {
		C:      "accounts",
		Id:     0,
		Insert: M{"balance": 200},
	}}

	err := s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)

	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)
}

func (s *S) TestInsertRemoveInsert(c *C) {
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Insert: M{"_id": 0, "balance": 200},
	}, {
		C:      "accounts",
		Id:     0,
		Remove: true,
	}, {
		C:      "accounts",
		Id:     0,
		Insert: M{"_id": 0, "balance": 300},
	}}

	err := s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)
}

func (s *S) TestErrors(c *C) {
	doc := bson.M{"foo": 1}
	tests := []txn.Op{{
		C:  "c",
		Id: 0,
	}, {
		C:      "c",
		Id:     0,
		Insert: doc,
		Remove: true,
	}, {
		C:      "c",
		Id:     0,
		Insert: doc,
		Update: doc,
	}, {
		C:      "c",
		Id:     0,
		Update: doc,
		Remove: true,
	}, {
		C:      "c",
		Assert: doc,
	}, {
		Id:     0,
		Assert: doc,
	}}

	txn.SetChaos(txn.Chaos{KillChance: 1.0})
	for _, op := range tests {
		c.Logf("op: %v", op)
		err := s.runner.Run([]txn.Op{op}, "")
		c.Assert(err, ErrorMatches, "error in transaction op 0: .*")
	}
}

func (s *S) TestAssertRefusesUpdate(c *C) {
	c.Assert(s.accounts.Insert(M{"_id": 0, "balance": 100}), IsNil)
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.D{{"balance", 200}},
		Update: bson.D{{"$inc", bson.D{{"balance", 100}}}},
	}}
	c.Assert(s.runner.Run(ops, ""), Equals, txn.ErrAborted)
	var account Account
	c.Assert(s.accounts.FindId(0).One(&account), IsNil)
	c.Assert(account.Balance, Equals, 100)
}

func (s *S) TestAssertNestedOr(c *C) {
	// Assert uses $or internally. Ensure nesting works.
	err := s.accounts.Insert(M{"_id": 0, "balance": 100})
	c.Assert(err, IsNil)

	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.D{{"$or", []bson.D{{{"balance", 100}}, {{"balance", 300}}}}},
		Update: bson.D{{"$inc", bson.D{{"balance", 100}}}},
	}}

	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)

	err = s.runner.Run(ops, "")
	c.Assert(err, Equals, txn.ErrAborted)

	ops2 := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.D{{"balance", 200}},
		Update: bson.D{{"$set", bson.D{{"balance", 300}}}},
	}}
	err = s.runner.Run(ops2, "")
	c.Assert(err, IsNil)

	// Now that we're at 300, the original ops should apply again
	err = s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 400)
}

func (s *S) TestInsertInvalidAssert(c *C) {
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.D{{"balance", 100}},
		Insert: bson.D{{"balance", 100}},
	}}

	err := s.runner.Run(ops, "")
	c.Assert(err, ErrorMatches, `Insert can only Assert txn.DocMissing not \[\{balance 100\}\]`)

	ops = []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocExists,
		Insert: bson.D{{"balance", 100}},
	}}

	err = s.runner.Run(ops, "")
	c.Assert(err, ErrorMatches, "Insert can only Assert txn.DocMissing not txn.DocExists")
}

func (s *S) TestVerifyFieldOrdering(c *C) {
	// Used to have a map in certain operations, which means
	// the ordering of fields would be messed up.
	fields := bson.D{{"a", 1}, {"b", 2}, {"c", 3}}
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Insert: fields,
	}}

	err := s.runner.Run(ops, "")
	c.Assert(err, IsNil)

	var d bson.D
	err = s.accounts.FindId(0).One(&d)
	c.Assert(err, IsNil)

	var filtered bson.D
	for _, e := range d {
		switch e.Name {
		case "a", "b", "c":
			filtered = append(filtered, e)
		}
	}
	c.Assert(filtered, DeepEquals, fields)
}

func (s *S) TestChangeLog(c *C) {
	c.Assert(s.db.C("people").Create(&mgo.CollectionInfo{}), IsNil)
	c.Assert(s.db.C("debts").Create(&mgo.CollectionInfo{}), IsNil)
	c.Assert(s.db.C("chglog").Create(&mgo.CollectionInfo{
		Capped:   true,
		MaxBytes: 1e6,
	}), IsNil)
	chglog := s.db.C("chglog")
	s.runner.ChangeLog(chglog)

	ops := []txn.Op{{
		C:      "debts",
		Id:     0,
		Assert: txn.DocMissing,
	}, {
		C:      "accounts",
		Id:     0,
		Insert: M{"balance": 300},
	}, {
		C:      "accounts",
		Id:     1,
		Insert: M{"balance": 300},
	}, {
		C:      "people",
		Id:     "joe",
		Insert: M{"accounts": []int64{0, 1}},
	}}
	id := bson.NewObjectId()
	err := s.runner.Run(ops, id)
	c.Assert(err, IsNil)

	type IdList []interface{}
	type Log struct {
		Docs   IdList  "d"
		Revnos []int64 "r"
	}
	var m map[string]*Log
	err = chglog.FindId(id).One(&m)
	c.Assert(err, IsNil)

	c.Assert(m["accounts"], DeepEquals, &Log{IdList{0, 1}, []int64{2, 2}})
	c.Assert(m["people"], DeepEquals, &Log{IdList{"joe"}, []int64{2}})
	c.Assert(m["debts"], IsNil)

	ops = []txn.Op{{
		C:      "accounts",
		Id:     0,
		Update: M{"$inc": M{"balance": 100}},
	}, {
		C:      "accounts",
		Id:     1,
		Update: M{"$inc": M{"balance": 100}},
	}}
	id = bson.NewObjectId()
	err = s.runner.Run(ops, id)
	c.Assert(err, IsNil)

	m = nil
	err = chglog.FindId(id).One(&m)
	c.Assert(err, IsNil)

	c.Assert(m["accounts"], DeepEquals, &Log{IdList{0, 1}, []int64{3, 3}})
	c.Assert(m["people"], IsNil)

	ops = []txn.Op{{
		C:      "accounts",
		Id:     0,
		Remove: true,
	}, {
		C:      "people",
		Id:     "joe",
		Remove: true,
	}}
	id = bson.NewObjectId()
	err = s.runner.Run(ops, id)
	c.Assert(err, IsNil)

	m = nil
	err = chglog.FindId(id).One(&m)
	c.Assert(err, IsNil)

	c.Assert(m["accounts"], DeepEquals, &Log{IdList{0}, []int64{-1}})
	c.Assert(m["people"], DeepEquals, &Log{IdList{"joe"}, []int64{-1}})
}

func (s *S) TestInsertStressTest(c *C) {
	if *fast {
		c.Skip("-fast was supplied and this test is slow")
	}
	// TODO: implement Chaos
	// txn.SetChaos(txn.Chaos{
	// 	SlowdownChance: 0.3,
	// 	Slowdown:       50 * time.Millisecond,
	// })
	// defer txn.SetChaos(txn.Chaos{})

	// So we can run more iterations of the test in less time.
	txn.SetDebug(false)

	const runners = 10
	const inserts = 10
	const repeat = 100

	logger := &testLogger{c}
	for r := 0; r < repeat; r++ {
		var wg sync.WaitGroup
		wg.Add(runners)
		for i := 0; i < runners; i++ {
			go func(i, r int) {
				defer wg.Done()

				session := s.session.New()
				defer session.Close()
				runner := sstxn.NewRunner(s.db.With(session), logger)

				for j := 0; j < inserts; j++ {
					ops := []txn.Op{{
						C:  "accounts",
						Id: fmt.Sprintf("insert-%d-%d", r, j),
						Insert: bson.M{
							"added-by": i,
						},
					}}
					err := runner.Run(ops, "")
					if err != txn.ErrAborted {
						c.Check(err, IsNil)
					}
				}
			}(i, r)
		}
		wg.Wait()
	}
}
