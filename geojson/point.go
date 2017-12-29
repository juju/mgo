package geojson

// Point is an implementation of the GeoJSON "Point" type.
type Point struct {
	Coordinates []float64 `bson:"coordinates"`
}

// GeoJSON implements geojson.Type - it has no functional use.
func (g *Point) GeoJSON() {}

// GetBSON converts g into the correct format for BSON marshalling.
func (g *Point) GetBSON() (interface{}, error) {
	return struct {
		Type        string    `bson:"type"`
		Coordinates []float64 `bson:"coordinates"`
	}{
		Type:        "Point",
		Coordinates: g.Coordinates,
	}, nil
}
