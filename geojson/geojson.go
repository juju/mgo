// Package geojson implements a subset of RFC7946 for geospatial operations.
//
// For a list of supported GeoJSON types within MongoDB see:
// 		https://docs.mongodb.com/manual/reference/geojson/
//
// For a the GeoJSON RFC specification see:
// 		https://tools.ietf.org/html/rfc7946
//
package geojson

// Type is used to group GeoJSON types.
type Type interface {
	// GetBSON provides a BSON encoded, correctly formatted GeoJSON object..
	GetBSON() (interface{}, error)

	// GeoJSON provides no functionality - it is used to provide type safety for
	// inputs into geospatial methods.
	GeoJSON()
}
