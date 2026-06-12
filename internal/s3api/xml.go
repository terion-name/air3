package s3api

import "encoding/xml"

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

func xmlRenderDocument(value any) ([]byte, error) {
	body, err := xml.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), body...), nil
}
