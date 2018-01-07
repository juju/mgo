package mgo

import (
	"fmt"

	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo/geojson"
)

func insertGeospatialExampleData(collection *Collection) {
	var data = []struct {
		Name     string         `bson:"name"`
		Rating   uint8          `bson:"rating"`
		Location *geojson.Point `bson:"location"`
	}{
		{
			Name:     "Average Grub",
			Rating:   2,
			Location: &geojson.Point{[]float64{51.528594, -0.090247}},
		},
		{
			Name:     "Fivestars",
			Rating:   5,
			Location: &geojson.Point{[]float64{51.524804, -0.093450}},
		},
		{
			Name:     "Salted Pig",
			Rating:   5,
			Location: &geojson.Point{[]float64{51.523760, -0.076285}},
		},
		{
			Name:     "Nearly Perfect",
			Rating:   4,
			Location: &geojson.Point{[]float64{51.537375, -0.075756}},
		},
		{
			Name:     "Food Express",
			Rating:   3,
			Location: &geojson.Point{[]float64{51.536131, -0.103877}},
		},
		{
			Name:     "Hearts",
			Rating:   3,
			Location: &geojson.Point{[]float64{52.954783, -1.158109}},
		},
	}

	for i := range data {
		if err := collection.Insert(data[i]); err != nil {
			panic(err)
		}
	}

	index := Index{Key: []string{"$2dsphere:location"}}
	if err := collection.EnsureIndex(index); err != nil {
		panic(err)
	}
}

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
	collection := session.DB("").C("food")

	// Example data setup/cleanup
	insertGeospatialExampleData(collection)
	defer collection.DropCollection()

	// Define the search position
	point := &geojson.Point{
		Coordinates: []float64{51.525534, -0.088230},
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

	resultSet, err := NewGeospatial(collection).Near(point, opts)
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

	fmt.Printf("\nAverage disstance: %2.2fkm", resultSet.Stats.AvgDistance/kmDivisor)

	// Output:
	// 1. 3.66km	Average Grub                        **
	// 2. 5.27km	Fivestars                           *****
	// 3. 12.08km	Salted Pig                          *****
	// 4. 17.20km	Nearly Perfect                      ****
	// 5. 18.90km	Food Express                        ***
	//
	// Average disstance: 11.42km
}
