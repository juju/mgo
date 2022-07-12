// Copyright 2018 Canonical Ltd.
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

// The sstxn package implements the txn/Runner interface for server-side transactions.
package sstxn

import (
	"fmt"

	"github.com/juju/mgo/v2"
	"github.com/juju/mgo/v2/bson"
	"github.com/juju/mgo/v2/txn"
)

// Logger defines the types of logging that we will be doing
type Logger interface {
	Tracef(message string, args ...interface{})
	Debugf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Errorf(message string, args ...interface{})
	Criticalf(message string, args ...interface{})
}

type nilLogger struct {
}

func (nilLogger) Tracef(message string, args ...interface{})    {}
func (nilLogger) Debugf(message string, args ...interface{})    {}
func (nilLogger) Infof(message string, args ...interface{})     {}
func (nilLogger) Warningf(message string, args ...interface{})  {}
func (nilLogger) Errorf(message string, args ...interface{})    {}
func (nilLogger) Criticalf(message string, args ...interface{}) {}

var _ Logger = nilLogger{}

// A Runner applies operations as part of a transaction onto any number
// of collections within a database. See the Run method for details.
type Runner struct {
	db             *mgo.Database
	logCollection  *mgo.Collection // log
	logger         Logger
	startHook      func()
	postAssertHook func()
}

// NewRunner returns a new transaction runner that uses tc to hold its
// transactions.
//
// Multiple transaction collections may exist in a single database, but
// all collections that are touched by operations in a given transaction
// collection must be handled exclusively by it.
//
// A second collection with the same name of tc but suffixed by ".stash"
// will be used for implementing the transactional behavior of insert
// and remove operations.
func NewRunner(db *mgo.Database, logger Logger) *Runner {
	if logger == nil {
		logger = nilLogger{}
	}
	return &Runner{
		db:            db,
		logCollection: nil,
		logger:        logger,
	}
}

// Run creates a new transaction with ops and runs it immediately.
// The id parameter specifies the transaction id, and may be written
// down ahead of time to later verify the success of the change and
// resume it, when the procedure is interrupted for any reason. If
// empty, a random id will be generated.
// The info parameter, is ignored, and only preserved for api compatibility
// with client-side transaction Runner.Run()
//
// Operations across documents are not atomically applied, but are
// guaranteed to be eventually all applied in the order provided or
// all aborted, as long as the affected documents are only modified
// through transactions. If documents are simultaneously modified
// by transactions and out of transactions the behavior is undefined.
//
// If Run returns no errors, all operations were applied successfully.
// If it returns ErrAborted, one or more operations can't be applied
// and the transaction was entirely aborted with no changes performed.
//
// Any number of transactions may be run concurrently, with one
// runner or many.
func (r *Runner) Run(ops []txn.Op, id bson.ObjectId, info interface{}) (err error) {
	const efmt = "error in transaction op %d: %s"
	for i := range ops {
		op := &ops[i]
		if op.C == "" || op.Id == nil {
			return fmt.Errorf(efmt, i, "C or Id missing")
		}
		changes := 0
		if op.Insert != nil {
			changes++
		}
		if op.Update != nil {
			changes++
		}
		if op.Remove {
			changes++
		}
		if changes > 1 {
			return fmt.Errorf(efmt, i, "more than one of Insert/Update/Remove set")
		}
		if changes == 0 && op.Assert == nil {
			return fmt.Errorf(efmt, i, "none of Assert/Insert/Update/Remove set")
		}
	}
	if id == "" {
		id = bson.NewObjectId()
	}

	// Sometimes the mongo server will return an error code 112 (write conflict).
	// This is a signal the transaction needs to be retried.
	// We'll retry 3 times but not forever.
	for i := 0; i < 3; i++ {
		err = r.runTxn(ops, id)
		if err == errWriteConflict {
			r.logger.Tracef("attempt %d retrying txn ops", i)
			continue
		}
		break
	}
	if err == errWriteConflict {
		err = txn.ErrAborted
	}
	return err
}

func (r *Runner) runTxn(ops []txn.Op, id bson.ObjectId) error {
	completed := false
	if err := r.db.Session.StartTransaction(); err != nil {
		return err
	}
	defer func() {
		if !completed {
			abortErr := r.db.Session.AbortTransaction()
			if abortErr != nil && abortErr.Error() != "no transaction in progress" {
				r.logger.Errorf("error while aborting: %v", abortErr)
			}
		}
	}()
	if r.startHook != nil {
		r.startHook()
	}
	var revnos []int64
	if revs, err := r.checkAsserts(ops); err != nil {
		return err
	} else {
		revnos = revs
	}
	if r.postAssertHook != nil {
		r.postAssertHook()
	}
	if err := r.applyOps(ops, revnos); err != nil {
		return err
	}
	if err := r.updateLog(ops, revnos, id); err != nil {
		return err
	}
	if err := r.db.Session.CommitTransaction(); mgo.IsTxnAborted(err) {
		return txn.ErrAborted
	} else if err != nil {
		return err
	}
	completed = true
	return nil
}

var idFields = bson.D{{Name: "_id", Value: 1}}
var revnoFields = bson.D{{Name: "txn-revno", Value: 1}}

type revnoDoc struct {
	Revno int64 `bson:"txn-revno,omitempty"`
}

func (r *Runner) checkAsserts(ops []txn.Op) ([]int64, error) {
	// TODO(jam): 2019-04-10 users of this API should have already read the
	//  documents and built their changes on it. Thus we shouldn't have to redo
	//  assertions here, which would let us avoid re-reading the documents.
	//  What we really want is for the TXN to live for the lifetime of the buildTxn function.
	//  So that once the build starts, we can ensure that the documents are read
	//  as part of the transaction, and that they won't be changing during the
	//  lifetime of all of those documents that are read.
	//  That said... if we are going to do assertion checking, we will read the
	//  revnos, because most of the docs should have an Assert, so we
	//  have to read them anyway. And this way we have the revnos for the txn.log
	revnos := make([]int64, len(ops))
	for i, op := range ops {
		c := r.db.C(op.C)
		var rDoc revnoDoc
		if op.Assert == nil {
			err := c.FindId(op.Id).Select(revnoFields).One(&rDoc)
			if err != nil {
				if err != mgo.ErrNotFound {
					return nil, err
				}
				revnos[i] = -1
			} else {
				revnos[i] = rDoc.Revno
			}
			continue
		}
		if op.Insert != nil && op.Assert != txn.DocMissing {
			if op.Assert == txn.DocExists {
				return nil, fmt.Errorf("Insert can only Assert txn.DocMissing not txn.DocExists")
			} else {
				return nil, fmt.Errorf("Insert can only Assert txn.DocMissing not %v", op.Assert)
			}
		}

		if op.Assert == txn.DocExists {
			if c.FindId(op.Id).Select(revnoFields).One(&rDoc) == mgo.ErrNotFound {
				r.logger.Tracef("DocExists assertion failed for op: %#v", op)
				return nil, txn.ErrAborted
			}
		} else if op.Assert == txn.DocMissing {
			if c.FindId(op.Id).Select(idFields).One(nil) != mgo.ErrNotFound {
				r.logger.Tracef("DocMissing assertion failed for op: %#v", op)
				return nil, txn.ErrAborted
			}
			rDoc.Revno = -1
		} else {
			// Client side txns used to assert on txn-revno not changing once
			// the document was 'prepared'. But we don't actually care, so
			// we can just run the Assert as given.
			qdoc := bson.D{{Name: "_id", Value: op.Id}}
			// client-side txns use $or here, seems an odd choice.
			qdoc = append(qdoc, bson.DocElem{"$or", []interface{}{op.Assert}})
			if c.Find(qdoc).Select(revnoFields).One(&rDoc) == mgo.ErrNotFound {
				r.logger.Tracef("assertion failed for op: %#v", op)
				return nil, txn.ErrAborted
			}
		}
		revnos[i] = rDoc.Revno
	}
	return revnos, nil
}

// objToDoc converts an arbitrary Struct/bson.D/bson.M to a pure bson.D by
// Marshalling and Unmarshalling the document. We do this so we can add the
// txn-revno field to the document.
func objToDoc(obj interface{}) (d bson.D, err error) {
	data, err := bson.Marshal(obj)
	if err != nil {
		return nil, err
	}
	err = bson.Unmarshal(data, &d)
	if err != nil {
		return nil, err
	}
	return d, err
}

// addToDoc will check if 'name' already exists in the doc, and if so, extend the
// contents with value. Otherwise, it will append a new parameter with that name
func addToDoc(doc bson.D, name string, value bson.D) (bson.D, error) {
	for i := range doc {
		if doc[i].Name == name {
			if old, ok := doc[i].Value.(bson.D); ok {
				doc[i].Value = append(old, value...)
				return doc, nil
			} else {
				return nil, fmt.Errorf("invalid %q value in change document: %#v", name, doc[i].Value)
			}
		}
	}
	doc = append(doc, bson.DocElem{Name: name, Value: value})
	return doc, nil
}

// setInDoc will check if 'name' already exists in the doc, and if so, replace
// the value with the new value. Otherwise, it will append a new parameter
func setInDoc(doc bson.D, name string, value interface{}) bson.D {
	for i := range doc {
		if doc[i].Name == name {
			doc[i].Value = value
			return doc
		}
	}
	doc = append(doc, bson.DocElem{Name: name, Value: value})
	return doc
}

func (r *Runner) applyOps(ops []txn.Op, revnos []int64) error {
	for i, op := range ops {
		var err error
		switch {
		case op.Update != nil:
			err = r.applyUpdate(op, &revnos[i])
		case op.Insert != nil:
			err = r.applyInsert(op, &revnos[i])
		case op.Remove:
			err = r.applyRemove(op, &revnos[i])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) applyUpdate(op txn.Op, revno *int64) error {
	c := r.db.C(op.C)
	r.logger.Tracef("update op: %#v", op)
	d, err := objToDoc(op.Update)
	if err != nil {
		return err
	}
	*revno++
	d, err = addToDoc(d, "$set", bson.D{{"txn-revno", *revno}})
	if err != nil {
		return err
	}
	err = c.UpdateId(op.Id, d)
	if err == nil {
		// happy path
		return nil
	} else if IsWriteConflict(err) {
		r.logger.Tracef("update op: %#v write conflict: %v", op, err)
		return errWriteConflict
	} else if mgo.IsDup(err) || mgo.IsTxnAborted(err) {
		// This has triggered a server-side abort, so make sure the user
		// understands the txn is aborted
		r.logger.Tracef("update op: %#v aborted: %v", op, err)
		return txn.ErrAborted
	} else {
		r.logger.Tracef("op %#v error: %v", op, err)
		return err
	}
}

// IsWriteConflict checks if the supplied error is a Mongo WriteConflict error.
// This usually indicates we had 2 transactions trying to write to the same document,
// but is also possible when the server side needs the transaction to be retried due
// to replica consistency issues.
func IsWriteConflict(err error) bool {
	if e, ok := err.(*mgo.QueryError); ok {
		if e.Code == 112 {
			return true
		}
	}
	return false
}

// errWriteConflict is used when a server side write conflict error occurs.
var errWriteConflict = fmt.Errorf("write conflict")

func (r *Runner) applyInsert(op txn.Op, revno *int64) error {
	c := r.db.C(op.C)
	r.logger.Tracef("inserting op: %#v", op)
	d, err := objToDoc(op.Insert)
	if err != nil {
		return err
	}
	d = setInDoc(d, "_id", op.Id)
	d = setInDoc(d, "txn-revno", 2)
	*revno = 2
	r.logger.Tracef("inserting d: %#v", d)
	err = c.Insert(d)
	if err == nil {
		// happy path
		return nil
	} else if IsWriteConflict(err) {
		r.logger.Tracef("insert op: %#v write conflict: %v", op, err)
		return errWriteConflict
	} else if mgo.IsDup(err) || mgo.IsTxnAborted(err) {
		// This has triggered a server-side abort, so make sure the user
		// understands the txn is aborted
		r.logger.Tracef("insert op: %#v aborted: %v", op, err)
		return txn.ErrAborted
	} else {
		r.logger.Tracef("op %#v error: %v", op, err)
		return err
	}
}

func (r *Runner) applyRemove(op txn.Op, revno *int64) error {
	c := r.db.C(op.C)
	r.logger.Tracef("remove op: %#v", op)
	err := c.RemoveId(op.Id)
	// We negate the revno to indicate it was deleted
	*revno = -(*revno + 1)
	if err == nil || err == mgo.ErrNotFound {
		// happy path
		// note that removing a non-existing object does *not* abort the transaction
		return nil
	} else if IsWriteConflict(err) {
		r.logger.Tracef("remove op: %#v write conflict: %v", op, err)
		return errWriteConflict
	} else if mgo.IsTxnAborted(err) {
		// This has triggered a server-side abort, so make sure the user
		// understands the txn is aborted
		r.logger.Tracef("insert op: %#v aborted: %v", op, err)
		return txn.ErrAborted
	} else {
		r.logger.Tracef("op %#v error: %v", op, err)
		return err
	}
}

func (r *Runner) updateLog(ops []txn.Op, revnos []int64, txnId bson.ObjectId) error {
	if r.logCollection == nil {
		return nil
	}
	logDoc := bson.D{{Name: "_id", Value: txnId}}
	for i, op := range ops {
		if op.Insert == nil && op.Update == nil && !op.Remove {
			// this is assert only, so it isn't considered a change
			continue
		}
		// Add change to the log document.
		var dr bson.D
		for li := range logDoc {
			elem := &logDoc[li]
			if elem.Name == op.C {
				dr = elem.Value.(bson.D)
				break
			}
		}
		if dr == nil {
			dr = bson.D{{"d", []interface{}{}}, {"r", []int64{}}}
			logDoc = append(logDoc, bson.DocElem{op.C, dr})
		}
		dr[0].Value = append(dr[0].Value.([]interface{}), op.Id)
		dr[1].Value = append(dr[1].Value.([]int64), revnos[i])
	}
	err := r.logCollection.Insert(logDoc)
	if err == nil {
		return nil
	} else if IsWriteConflict(err) {
		r.logger.Tracef("insert op: txn log write conflict: %v", err)
		return errWriteConflict
	} else if mgo.IsDup(err) || mgo.IsTxnAborted(err) {
		// This has triggered a server-side abort, so make sure the user
		// understands the txn is aborted
		r.logger.Tracef("insert op: txn log aborted: %v", err)
		return txn.ErrAborted
	} else {
		r.logger.Tracef("insert op txn log error: %v", err)
		return err
	}
	return nil
}

// ChangeLog enables logging of changes to the given collection
// every time a transaction that modifies content is done being
// applied.
//
// Saved documents are in the format:
//
//     {"_id": <txn id>, <collection>: {"d": [<doc id>, ...], "r": [<doc revno>, ...]}}
//
// The document revision is the value of the txn-revno field after
// the change has been applied. Negative values indicate the document
// was not present in the collection. Revisions will not change when
// updates or removes are applied to missing documents or inserts are
// attempted when the document isn't present.
func (r *Runner) ChangeLog(logc *mgo.Collection) {
	r.logCollection = logc
}

// ResumeAll is a no-op on server-side transactions because there is nothing to resume.
func (r *Runner) ResumeAll() error {
	return nil
}

// SetStartHook will call func() as soon as StartTransaction is called.
// This can be used to play games before the Assert check.
func (r *Runner) SetStartHook(hook func()) {
	r.startHook = hook
}

// SetPostAssertHook will call func() after we have done Assertions, but
// before we actually make any database changes.
func (r *Runner) SetPostAssertHook(hook func()) {
	r.postAssertHook = hook
}
