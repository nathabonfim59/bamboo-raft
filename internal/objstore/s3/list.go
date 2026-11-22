// ListObjectsV2: pagination, URL-encoded keys, ascending order.
package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"time"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore"
)

// maxListBody bounds one ListObjectsV2 response (1000 entries).
const maxListBody = 16 << 20

// listBucketResult is the ListObjectsV2 response document.
type listBucketResult struct {
	XMLName               xml.Name    `xml:"ListBucketResult"`
	IsTruncated           bool        `xml:"IsTruncated"`
	Contents              []listEntry `xml:"Contents"`
	NextContinuationToken string      `xml:"NextContinuationToken"`
}

// listEntry is one object in a ListObjectsV2 response. With
// encoding-type=url, Key is percent-encoded.
type listEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

// listObjects yields prefix's objects in ascending key order, one page at a
// time. An error is yielded once, as the last pair.
func (s *Store) listObjects(ctx context.Context, prefix string) iter.Seq2[objstore.Info, error] {
	return func(yield func(objstore.Info, error) bool) {
		token := ""
		for {
			if err := ctx.Err(); err != nil {
				yield(objstore.Info{}, err)
				return
			}
			query := url.Values{
				"list-type":     {"2"},
				"encoding-type": {"url"},
				"max-keys":      {"1000"},
			}
			if prefix != "" {
				query.Set("prefix", prefix)
			}
			if token != "" {
				query.Set("continuation-token", token)
			}
			req, err := s.newRequest(ctx, http.MethodGet, "", query)
			if err != nil {
				yield(objstore.Info{}, err)
				return
			}
			resp, err := s.do(ctx, req, emptyPayloadHash, false)
			if err != nil {
				yield(objstore.Info{}, fmt.Errorf("s3: list %q: %w", prefix, err))
				return
			}
			result, err := s.decodeList(prefix, resp)
			resp.Body.Close()
			if err != nil {
				yield(objstore.Info{}, err)
				return
			}
			for _, entry := range result.Contents {
				key, err := url.QueryUnescape(entry.Key)
				if err != nil {
					yield(objstore.Info{}, fmt.Errorf("s3: list %q: undecodable key %q: %w", prefix, entry.Key, err))
					return
				}
				info := objstore.Info{Key: key, Size: entry.Size}
				if entry.LastModified != "" {
					if t, err := time.Parse(time.RFC3339, entry.LastModified); err == nil {
						info.Modified = t
					}
				}
				if !yield(info, nil) {
					return
				}
			}
			if !result.IsTruncated {
				return
			}
			if result.NextContinuationToken == "" {
				yield(objstore.Info{}, fmt.Errorf("s3: list %q: truncated response without a continuation token", prefix))
				return
			}
			token = result.NextContinuationToken
		}
	}
}

// decodeList parses one page, mapping a non-2xx response to an error.
func (s *Store) decodeList(prefix string, resp *http.Response) (listBucketResult, error) {
	if resp.StatusCode/100 != 2 {
		return listBucketResult{}, s.apiErrorFrom("list", prefix, resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListBody+1))
	if err != nil {
		return listBucketResult{}, fmt.Errorf("s3: list %q: read response: %w", prefix, err)
	}
	if len(body) > maxListBody {
		return listBucketResult{}, fmt.Errorf("s3: list %q: response exceeds %d bytes", prefix, maxListBody)
	}
	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return listBucketResult{}, fmt.Errorf("s3: list %q: decode response: %w", prefix, err)
	}
	return result, nil
}
