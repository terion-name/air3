// Package edgesign creates and verifies air3 edge signed object URLs.
//
// These URLs are verified by the edge gateway. They are not S3 SigV4
// presigned URLs and should be generated with the same shared secret that the
// edge gateway uses for AIR3_EDGE_SIGNING_SECRET.
package edgesign
