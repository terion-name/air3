package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"sort"
)

const xmlS3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// ErrorResponse contains the S3 error fields rendered in an XML Error body.
type ErrorResponse struct {
	Code      string
	Message   string
	Resource  string
	RequestID string
}

// RenderErrorXML renders an S3 Error XML document with a deterministic header
// and compact body.
func RenderErrorXML(response ErrorResponse) ([]byte, error) {
	return xmlRenderDocument(xmlErrorResponse{
		Code:      response.Code,
		Message:   response.Message,
		Resource:  response.Resource,
		RequestID: response.RequestID,
	})
}

// ListObject contains one object entry in a ListBucketResult XML response.
type ListObject struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	StorageClass string
}

// ListBucketResult contains the S3 ListObjectsV2 fields rendered by this
// package.
type ListBucketResult struct {
	Name                  string
	Prefix                string
	Delimiter             string
	KeyCount              int
	MaxKeys               int
	IsTruncated           bool
	Contents              []ListObject
	CommonPrefixes        []string
	ContinuationToken     string
	NextContinuationToken string
	StartAfter            string
	EncodingType          string
}

// RenderListBucketResult renders a deterministic S3 ListBucketResult XML
// document with the S3 namespace on the root element.
func RenderListBucketResult(result ListBucketResult) ([]byte, error) {
	contents := make([]xmlListObject, 0, len(result.Contents))
	for _, object := range result.Contents {
		contents = append(contents, xmlListObject{
			Key:          object.Key,
			LastModified: object.LastModified,
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: object.StorageClass,
		})
	}

	commonPrefixes := make([]xmlCommonPrefix, 0, len(result.CommonPrefixes))
	for _, prefix := range result.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, xmlCommonPrefix{Prefix: prefix})
	}

	return xmlRenderDocument(xmlListBucketResult{
		XMLNS:                 xmlS3Namespace,
		Name:                  result.Name,
		Prefix:                result.Prefix,
		KeyCount:              result.KeyCount,
		MaxKeys:               result.MaxKeys,
		IsTruncated:           result.IsTruncated,
		Contents:              contents,
		CommonPrefixes:        commonPrefixes,
		NextContinuationToken: result.NextContinuationToken,
		ContinuationToken:     result.ContinuationToken,
		StartAfter:            result.StartAfter,
		Delimiter:             result.Delimiter,
		EncodingType:          result.EncodingType,
	})
}

// ApplyListPublicPrefix prepends publicKeyPrefix to object keys and common
// prefixes in result. Bucket names, request prefixes, and continuation tokens
// are left unchanged.
func ApplyListPublicPrefix(result *ListBucketResult, publicKeyPrefix string) {
	if result == nil {
		return
	}

	for i := range result.Contents {
		result.Contents[i].Key = publicKeyPrefix + result.Contents[i].Key
	}
	for i := range result.CommonPrefixes {
		result.CommonPrefixes[i] = publicKeyPrefix + result.CommonPrefixes[i]
	}
}

// InitiateMultipartUploadResult contains the CreateMultipartUpload response
// fields rendered by this package.
type InitiateMultipartUploadResult struct {
	Bucket   string
	Key      string
	UploadID string
}

// RenderInitiateMultipartUploadResult renders a deterministic S3
// InitiateMultipartUploadResult XML document.
func RenderInitiateMultipartUploadResult(result InitiateMultipartUploadResult) ([]byte, error) {
	return xmlRenderDocument(xmlInitiateMultipartUploadResult{
		XMLNS:    xmlS3Namespace,
		Bucket:   result.Bucket,
		Key:      result.Key,
		UploadID: result.UploadID,
	})
}

// CompleteMultipartUploadResult contains the CompleteMultipartUpload response
// fields rendered by this package.
type CompleteMultipartUploadResult struct {
	Location string
	Bucket   string
	Key      string
	ETag     string
}

// RenderCompleteMultipartUploadResult renders a deterministic S3
// CompleteMultipartUploadResult XML document.
func RenderCompleteMultipartUploadResult(result CompleteMultipartUploadResult) ([]byte, error) {
	return xmlRenderDocument(xmlCompleteMultipartUploadResult{
		XMLNS:    xmlS3Namespace,
		Location: result.Location,
		Bucket:   result.Bucket,
		Key:      result.Key,
		ETag:     result.ETag,
	})
}

// CompletedPart is one part entry parsed from a CompleteMultipartUpload
// request body.
type CompletedPart struct {
	PartNumber int32
	ETag       string
}

const maxCompleteMultipartParts = 10000

// ParseCompleteMultipartUpload parses a client CompleteMultipartUpload XML
// body of at most maxBytes, returning parts sorted by part number. Unknown
// elements (checksum members and the like) are ignored; part numbers must be
// unique and within 1..10000 and every part needs an ETag.
func ParseCompleteMultipartUpload(r io.Reader, maxBytes int64) ([]CompletedPart, error) {
	var body xmlCompleteMultipartUpload
	if err := xml.NewDecoder(io.LimitReader(r, maxBytes)).Decode(&body); err != nil {
		return nil, fmt.Errorf("parse CompleteMultipartUpload: %w", err)
	}
	if len(body.Parts) == 0 {
		return nil, fmt.Errorf("parse CompleteMultipartUpload: at least one part is required")
	}
	if len(body.Parts) > maxCompleteMultipartParts {
		return nil, fmt.Errorf("parse CompleteMultipartUpload: too many parts")
	}

	parts := make([]CompletedPart, 0, len(body.Parts))
	seen := make(map[int32]bool, len(body.Parts))
	for _, part := range body.Parts {
		if part.PartNumber < 1 || part.PartNumber > maxCompleteMultipartParts {
			return nil, fmt.Errorf("parse CompleteMultipartUpload: part number %d out of range 1..10000", part.PartNumber)
		}
		if part.ETag == "" {
			return nil, fmt.Errorf("parse CompleteMultipartUpload: part %d is missing an ETag", part.PartNumber)
		}
		if seen[part.PartNumber] {
			return nil, fmt.Errorf("parse CompleteMultipartUpload: duplicate part number %d", part.PartNumber)
		}
		seen[part.PartNumber] = true
		parts = append(parts, CompletedPart{PartNumber: part.PartNumber, ETag: part.ETag})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	return parts, nil
}

type xmlErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

type xmlListBucketResult struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	XMLNS                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []xmlListObject   `xml:"Contents"`
	CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
}

type xmlListObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type xmlCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type xmlInitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type xmlCompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type xmlCompleteMultipartUpload struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []xmlCompletedPart `xml:"Part"`
}

type xmlCompletedPart struct {
	PartNumber int32  `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

func xmlRenderDocument(value any) ([]byte, error) {
	body, err := xml.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), body...), nil
}
