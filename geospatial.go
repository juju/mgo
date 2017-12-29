package mgo

import (
	"errors"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo/geojson"
)

// Geospatial performs geospatial query operations on a given collection.
type Geospatial struct {
	collection *Collection
}

// NearOpts specifies the behaviour of a geoNear query.
//
// For documentation of the available options, see:
// 		https://docs.mongodb.com/manual/reference/command/geoNear/#dbcmd.geoNear
//
type NearOpts struct {
	// DocumentFactory should return a type representing a matched document.
	//
	// If no implementation is provided, the matched document is unmarshalled
	// into a bson.M. See ExampleGeospatial_Near for example usage.
	DocumentFactory func() interface{} `bson:"-"`

	Spherical          bool   `bson:"spherical,omitempty"`
	Limit              int64  `bson:"limit,omitempty"`
	MinDistance        int64  `bson:"minDistance,omitempty"`
	MaxDistance        int64  `bson:"maxDistance,omitempty"`
	Query              bson.M `bson:"query,omitempty"`
	DistanceMultiplier int64  `bson:"distanceMultiplier,omitempty"`
	IncludeLocs        bool   `bson:"includeLocs"`
	UniqueDocs         bool   `bson:"uniqueDocs,omitempty"`

	// TODO: read concern
}

// NearOutput defines the returned values from a geoNear query.
//
// For documentation of the returned types see:
// 		https://docs.mongodb.com/manual/reference/command/geoNear/#output
//
type NearOutput struct {
	Results []NearResult `bson:"results"`
	Stats   struct {
		NScanned      int64         `bson:"nscanned"`
		ObjectsLoaded int64         `bson:"objectsLoaded"`
		AvgDistance   float64       `bson:"avgDistance"`
		MaxDistance   float64       `bson:"maxDistance"`
		Time          time.Duration `bson:"time"`
	} `bson:"stats"`

	factory func() interface{}
}

// NearResult represents a single matched document for a geoNear query.
//
// Distance is expressed as radial degrees. The Document field is set to the
// value returned by NearOpts.DocumentFactory during unmarshalling, or a bson.M
// if DocumentFactory was not set.
type NearResult struct {
	Distance float64 `bson:"dis"`

	// Document is set to the result of calling Unmarshal on the matching record
	// into a value returned from NearOpts.DocumentFactory
	Document interface{} `bson:"-"`
}

// NewGeospatial returns a Geospatial instance using collection as the
// datasource.
func NewGeospatial(collection *Collection) *Geospatial {
	return &Geospatial{
		collection: collection,
	}
}

// Near performs a geoNear query against g.
func (g *Geospatial) Near(p geojson.Type, opts *NearOpts) (*NearOutput, error) {
	cmd := struct {
		Collection string       `bson:"geoNear"`
		Near       geojson.Type `bson:"near"`
		NearOpts   `bson:",inline"`
	}{
		Collection: g.collection.Name,
		Near:       p,
		NearOpts:   *opts,
	}

	results := &NearOutput{
		factory: opts.DocumentFactory,
	}

	return results, g.collection.Database.Run(cmd, results)
}

// SetBSON implements BSON unmarshalling of the query results.
func (n *NearOutput) SetBSON(raw bson.Raw) error {
	// Unmarshal into a raw document collection so the elements can be handled
	// manually
	var input bson.RawD
	if err := raw.Unmarshal(&input); err != nil {
		return err
	}

	// The loop below handles each element in the geoNear response explicitly.
	// At time of writing, a successful geoNear response from 3.4.10 looks like:
	//
	//		{
	//			"results" : [
	//				{
	//					"dis" : 0,
	//					"obj" : {
	//						"_id" : 2,
	//						"location" : { "type" : "Point", "coordinates" : [ -73.9667, 40.78 ] },
	//						"name" : "Central Park",
	//						...more document fields...
	//					}
	//				},
	//				{
	//					...etc...
	//				},
	//			],
	//			"stats" : {
	//				"nscanned" : NumberLong(47),
	//				"objectsLoaded" : NumberLong(47),
	//				"avgDistance" : 3450.8316469132747,
	//				"maxDistance" : 7106.506152782733,
	//				"time" : 4
	//			},
	//			"ok" : 1
	//		}

	var queryErr error
	for i := range input {
		switch input[i].Name {
		case "results":
			// Temporary structure to grab the document as raw BSON, and it's
			// corresponding inlined fields (distance).
			var res = []struct {
				NearResult `bson:",inline"`
				Object     bson.Raw `bson:"obj"`
			}{}

			if err := input[i].Value.Unmarshal(&res); err != nil {
				return err
			}

			// If there's no factory, the caller doesn't want to unmarshal into
			// a custom struct so a bson.M is used
			if n.factory == nil {
				n.factory = func() interface{} {
					return bson.M{}
				}
			}

			// Allocate enough NearResults in the NearOutput to hold everything
			// in the query result set
			n.Results = make([]NearResult, len(res))

			// Iterate over the query results, calling unmarshal on Object (the
			// matched document as a bson.Raw) into the type returned by the
			// factory
			for j := range res {
				x := n.factory()
				if err := res[j].Object.Unmarshal(x); err != nil {
					return err
				}

				// Allow the GC to collect the raw bytes
				res[j].Object.Data = nil

				// Move the result metadata contained within NearResult
				// (distance) and the actual document to the results slice that
				// is returned to the caller
				n.Results[j] = res[j].NearResult
				n.Results[j].Document = x
			}

		case "stats":
			// Unmarshal the stats as a whole
			if err := input[i].Value.Unmarshal(&n.Stats); err != nil {
				return err
			}

		case "errmsg":
			// Return the error message for a failed query
			var msg string
			if err := input[i].Value.Unmarshal(&n.Stats); err != nil {
				return err
			}
			queryErr = errors.New(msg)

		case "ok", "code", "codeName":
			// Ignored fields

		default:
			debugf("skipping unknown geoNear field %q in response", input[i].Name)
		}
	}

	return queryErr
}
