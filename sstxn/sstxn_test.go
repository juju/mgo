// Copyright 2018 Canonical Ltd.

package sstxn_test

import (
	"flag"
	"fmt"
	"sync"
	"testing"

	"github.com/juju/mgo/v3"
	"github.com/juju/mgo/v3/bson"
	"github.com/juju/mgo/v3/dbtest"
	"github.com/juju/mgo/v3/sstxn"
	"github.com/juju/mgo/v3/txn"
	. "gopkg.in/check.v1"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

var fast = flag.Bool("fast", false, "Skip slow tests")

type S struct {
	server          dbtest.DBServer
	externalSession *mgo.Session
	session         *mgo.Session
	db              *mgo.Database
	txnlog          *mgo.Collection
	accounts        *mgo.Collection
	runner          *sstxn.Runner
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
	c.Logf("wiping db server")
	s.server.Wipe()

	// Check if someone already has done 'make startdb'. If they have,
	// then we will use the HA mongo with a replicaset, since it
	// takes much less time to clean it up than to start a new replicaset.
	// If they don't then we'll run our own server.
	session, err := mgo.Dial("localhost:40011")
	if err == nil {
		c.Logf("using existing database at: %v", session.LiveServers())
		s.externalSession = session.Copy()
		s.session = session
	} else {
		c.Logf("unable to connect to :40011 using local server: %v", err)
		s.session = s.server.Session()
	}
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
	s.server.Wipe()
	if s.externalSession != nil {
		names, err := s.externalSession.DatabaseNames()
		if err != nil {
			panic(err)
		}
		for _, name := range names {
			switch name {
			case "admin", "local", "config":
			default:
				err = s.externalSession.DB(name).DropDatabase()
				if err != nil {
					panic(err)
				}
			}
		}
		s.externalSession.Close()
	}
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

	err = s.runner.Run(exists, "", nil)
	c.Assert(err, IsNil)
	err = s.runner.Run(missing, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)

	err = s.accounts.RemoveId(0)
	c.Assert(err, IsNil)

	err = s.runner.Run(exists, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)
	err = s.runner.Run(missing, "", nil)
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
	err = s.runner.Run(ops, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)

	ops[0].Id = 1
	err = s.runner.Run(ops, "", nil)
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

	err := s.runner.Run(ops, "", nil)
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

	err = s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)

	err = s.accounts.FindId(0).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// Removing a non-existing doc does not abort the transaction,
	// so we preserve the behavior.
	err = s.runner.Run(ops, "", nil)
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

	err = s.runner.Run(ops, "", nil)
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

	err := s.runner.Run(ops, "", nil)
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
	err = s.runner.Run(ops, "", nil)
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

	err := s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)

	err = s.runner.Run(ops, "", nil)
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

	err := s.runner.Run(ops, "", nil)
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
		err := s.runner.Run([]txn.Op{op}, "", nil)
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
	c.Assert(s.runner.Run(ops, "", nil), Equals, txn.ErrAborted)
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

	err = s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)

	err = s.runner.Run(ops, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)

	ops2 := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.D{{"balance", 200}},
		Update: bson.D{{"$set", bson.D{{"balance", 300}}}},
	}}
	err = s.runner.Run(ops2, "", nil)
	c.Assert(err, IsNil)

	// Now that we're at 300, the original ops should apply again
	err = s.runner.Run(ops, "", nil)
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

	err := s.runner.Run(ops, "", nil)
	c.Assert(err, ErrorMatches, `Insert can only Assert txn.DocMissing not \[\{balance 100\}\]`)

	ops = []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocExists,
		Insert: bson.D{{"balance", 100}},
	}}

	err = s.runner.Run(ops, "", nil)
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

	err := s.runner.Run(ops, "", nil)
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
	c.Assert(s.db.C("chglog").Create(&mgo.CollectionInfo{}), IsNil)
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
	err := s.runner.Run(ops, id, nil)
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
	err = s.runner.Run(ops, id, nil)
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
	err = s.runner.Run(ops, id, nil)
	c.Assert(err, IsNil)

	m = nil
	err = chglog.FindId(id).One(&m)
	c.Assert(err, IsNil)

	c.Assert(m["accounts"], DeepEquals, &Log{IdList{0}, []int64{-4}})
	c.Assert(m["people"], DeepEquals, &Log{IdList{"joe"}, []int64{-3}})
}

func (s *S) TestInsertStressTest(c *C) {
	if *fast {
		c.Skip("-fast was supplied and this test is slow")
	}

	// So we can run more iterations of the test in less time.
	txn.SetDebug(false)

	const (
		runners = 10
		inserts = 10
		repeat  = 100
	)

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
					err := runner.Run(ops, "", nil)
					if err != txn.ErrAborted {
						c.Check(err, IsNil)
					}
				}
			}(i, r)
		}
		wg.Wait()
	}
}

func (s *S) TestConcurrentUpdateTest(c *C) {
	txn.SetDebug(false)
	const (
		concurrent = 10
		count      = 10
	)
	s.accounts.Insert(bson.M{
		"_id":     0,
		"balance": 1,
	})
	var wg sync.WaitGroup
	wg.Add(concurrent)
	for i := 0; i < concurrent; i++ {
		go func(i int, session *mgo.Session) {
			defer wg.Done()
			defer session.Close()
			for j := 0; j < count; j++ {
				db := session.DB("test")
				runner := sstxn.NewRunner(db, &testLogger{c})
				err := runner.Run([]txn.Op{{
					C:      "accounts",
					Id:     0,
					Assert: txn.DocExists,
					Update: bson.M{"$inc": bson.M{"balance": 1}},
				}}, "", nil)
				c.Logf("routine %d: attempt: %d Update: %v", i, j, err)
				if err != nil {
					c.Check(err, Equals, txn.ErrAborted)
				}
			}
		}(i, s.session.Copy())
	}
	wg.Wait()
}

func (s *S) TestConcurrentInsertPreAssertFailure(c *C) {
	logger := &testLogger{c}
	runner1 := sstxn.NewRunner(s.db, logger)
	session2 := s.session.Copy()
	defer session2.Close()
	runner2 := sstxn.NewRunner(s.db.With(session2), logger)
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocMissing,
		Insert: bson.M{"foo": "bar"},
	}}
	runner1.SetStartHook(func() {
		err := runner2.Run(ops, "", nil)
		c.Check(err, IsNil)
	})
	err := runner1.Run(ops, "", nil)
	c.Check(err, Equals, txn.ErrAborted)
}

func (s *S) TestConcurrentInsertPostAssertFailure(c *C) {
	logger := &testLogger{c}
	runner1 := sstxn.NewRunner(s.db, logger)
	session2 := s.session.Copy()
	defer session2.Close()
	runner2 := sstxn.NewRunner(s.db.With(session2), logger)
	ops := []txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocMissing,
		Insert: bson.M{"foo": "bar"},
	}}
	runner1.SetPostAssertHook(func() {
		err := runner2.Run(ops, "", nil)
		c.Check(err, IsNil)
	})
	err := runner1.Run(ops, "", nil)
	c.Check(err, Equals, txn.ErrAborted)
}

func (s *S) TestConcurrentUpdatePreAssertFailure(c *C) {
	logger := &testLogger{c}
	runner1 := sstxn.NewRunner(s.db, logger)
	session2 := s.session.Copy()
	defer session2.Close()
	runner2 := sstxn.NewRunner(s.db.With(session2), logger)
	runner1.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocMissing,
		Insert: bson.M{"balance": 0},
	}}, "", nil)
	runner1.SetStartHook(func() {
		err := runner2.Run([]txn.Op{{
			C:      "accounts",
			Id:     0,
			Assert: txn.DocExists,
			Update: bson.M{"$set": bson.M{"balance": 1}},
		}}, "", nil)
		c.Check(err, IsNil)
	})
	err := runner1.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.M{"balance": 0},
		Update: bson.M{"$inc": bson.M{"balance": 1}},
	}}, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)
}

func (s *S) TestConcurrentUpdatePostAssertFailure(c *C) {
	logger := &testLogger{c}
	runner1 := sstxn.NewRunner(s.db, logger)
	session2 := s.session.Copy()
	defer session2.Close()
	runner2 := sstxn.NewRunner(s.db.With(session2), logger)
	runner1.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocMissing,
		Insert: bson.M{"balance": 0},
	}}, "", nil)
	runner1.SetPostAssertHook(func() {
		err := runner2.Run([]txn.Op{{
			C:      "accounts",
			Id:     0,
			Assert: txn.DocExists,
			Update: bson.M{"$set": bson.M{"balance": 1}},
		}}, "", nil)
		c.Check(err, IsNil)
	})
	err := runner1.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: bson.M{"balance": 0},
		Update: bson.M{"$inc": bson.M{"balance": 1}},
	}}, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)
}

func (s *S) TestConcurrentRemoveUpdatePostAssertFailure(c *C) {
	// If you update before we remove, the remove is aborted
	logger := &testLogger{c}
	runner1 := sstxn.NewRunner(s.db, logger)
	session2 := s.session.Copy()
	defer session2.Close()
	runner2 := sstxn.NewRunner(s.db.With(session2), logger)
	err := runner1.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Assert: txn.DocMissing,
		Insert: bson.M{"balance": 0},
	}}, "", nil)
	c.Assert(err, IsNil)
	runner1.SetPostAssertHook(func() {
		err := runner2.Run([]txn.Op{{
			C:      "accounts",
			Id:     0,
			Assert: txn.DocExists,
			Update: bson.M{"$inc": bson.M{"balance": 1}},
		}}, "", nil)
		c.Check(err, IsNil)
	})
	err = runner1.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Remove: true,
	}}, "", nil)
	// Since we are getting a WriteConflict, we retry for 120 seconds
	// and then fail with timeout error.
	c.Assert(err, Equals, sstxn.ErrTimeout)
}

type NotMarshallable struct {
	Error error
}

func (u NotMarshallable) GetBSON() (interface{}, error) {
	return nil, u.Error
}

func (u NotMarshallable) SetBSON(raw bson.Raw) error {
	return u.Error
}

func (s *S) TestNotMarshallableUpdate(c *C) {
	err := s.accounts.Insert(bson.D{{"_id", 0}, {"balance", 300}})
	c.Assert(err, IsNil)
	logger := &testLogger{c}
	runner := sstxn.NewRunner(s.db, logger)
	err = runner.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Update: NotMarshallable{Error: fmt.Errorf("cannot marshall for update")},
	}}, "", nil)
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, "cannot marshall for update")
	var raw bson.D
	err = s.accounts.FindId(0).One(&raw)
	c.Assert(err, IsNil)
	// Make sure the content hasn't been changed
	c.Check(raw, DeepEquals, bson.D{{"_id", 0}, {"balance", 300}})
}

func (s *S) TestNotMarshallableInsert(c *C) {
	logger := &testLogger{c}
	runner := sstxn.NewRunner(s.db, logger)
	err := runner.Run([]txn.Op{{
		C:      "accounts",
		Id:     0,
		Insert: NotMarshallable{Error: fmt.Errorf("cannot marshall for insert")},
	}}, "", nil)
	c.Assert(err, NotNil)
	c.Check(err, ErrorMatches, "cannot marshall for insert")
	var raw bson.D
	err = s.accounts.FindId(0).One(&raw)
	c.Check(err, Equals, mgo.ErrNotFound)
}

type testStruct struct {
	DocID string `bson:"_id"`
	Value string `bson:"value"`
}

func (s *S) TestInsertValueFromStructForcedID(c *C) {
	// If someone only sets the Value part of a document, we still want to make
	// sure the _id of the inserted document is correct.
	c.Assert(s.runner.Run([]txn.Op{{
		C:      "accounts",
		Id:     "1",
		Assert: txn.DocMissing,
		Insert: testStruct{Value: "value"},
	}}, "", nil), IsNil)
	var res testStruct
	c.Assert(s.accounts.FindId("1").One(&res), IsNil)
	c.Check(res.DocID, Equals, "1")
	c.Check(res.Value, Equals, "value")
}
