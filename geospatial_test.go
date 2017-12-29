package mgo

import (
	"fmt"

	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo/geojson"
)

func ExampleGeospatial_Near() {
	// An example document structure
	type Restaurant struct {
		Name   string
		Rating uint8
	}

	// Connect to mongo
	session, err := Dial("localhost:27017")
	if err != nil {
		panic(err)
	}

	// Define the search position
	point := &geojson.Point{
		Coordinates: []float64{-73.93414657, 40.82302903},
	}

	// Define the query options (return closest 5, where rating is at least 1)
	opts := &NearOpts{
		Spherical: true,
		Limit:     5,
		Query: bson.M{
			"rating": bson.M{
				"$gte": 1,
			},
		},

		// DocumentFactory returns an empty type to Unmarshal the returned
		// document into (NearResult.Document is set to this value and can be
		// typecast safely).
		//
		// If DocumentFactory is not set, the matched document is unmarsahlled
		// into a bson.M type.
		DocumentFactory: func() interface{} {
			return &Restaurant{}
		},
	}

	resultSet, err := NewGeospatial(session.DB("").C("restaurants")).Near(point, opts)
	if err != nil {
		panic(err)
	}

	var stars = []string{
		"",
		"*",
		"**",
		"***",
		"****",
		"*****",
	}

	// Crude conversion of radial degrees into km.
	//
	// Wikipedia says 1 degree is 111.32km at the equator - see
	// https://en.wikipedia.org/wiki/Decimal_degrees
	const kmDivisor = 111.32

	// Print out the results
	for i, r := range resultSet.Results {
		var doc = r.Document.(*Restaurant)
		fmt.Printf("%d. %2.2fkm\t%-35s %s\n", i+1, r.Distance/kmDivisor, doc.Name, stars[doc.Rating])
	}

	fmt.Printf("\nQuery took %v", resultSet.Stats)

	// Output:
	// 1. 0.00km	Fivestars                           *****
	// 2. 1.19km	Nearly Great                        ****
	// 3. 3.37km	Salted Pig                          *****
	// 4. 5.20km	Escherichia.C                       *
	// 5. 5.92km	Hearts                              ***
	//
	// Query took 1.42s
}
