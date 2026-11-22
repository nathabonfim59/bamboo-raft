// Integration tests for the S3 store: the objstore conformance suite against
// a local Garage and, when configured, against a real S3 service.
package s3_test

import (
	"cmp"
	"os"
	"strconv"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/objstore/objstoretest"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3"
	"github.com/nathabonfim59/bamboo-raft/internal/objstore/s3/s3test"
)

// TestGarageConformance runs the whole objstore suite against a single-node
// Garage started by s3test.StartGarage. It skips unless
// BBRAFT_TEST_S3_GARAGE=1 and the garage binary is in PATH;
// BBRAFT_TEST_REQUIRE_ALL=1 makes either missing piece a failure.
func TestGarageConformance(t *testing.T) {
	cfg := s3test.StartGarage(t)
	store, err := s3.New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	objstoretest.Run(t, store, objstoretest.Options{Presign: true})
}

// TestRealServiceConformance runs the whole objstore suite against the service
// in BBRAFT_TEST_S3_ENDPOINT, with BBRAFT_TEST_S3_BUCKET,
// BBRAFT_TEST_S3_ACCESS_KEY_ID and BBRAFT_TEST_S3_SECRET_ACCESS_KEY.
// BBRAFT_TEST_S3_REGION defaults to us-east-1 and BBRAFT_TEST_S3_PATH_STYLE
// (a strconv.ParseBool value) defaults to false, which suits AWS S3; set it
// for services such as R2 that want the bucket in the path. The bucket is
// used as-is and must be empty or hold only keys outside the suite's random
// prefix. It skips when the variables are unset.
func TestRealServiceConformance(t *testing.T) {
	endpoint := os.Getenv("BBRAFT_TEST_S3_ENDPOINT")
	bucket := os.Getenv("BBRAFT_TEST_S3_BUCKET")
	accessKeyID := os.Getenv("BBRAFT_TEST_S3_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("BBRAFT_TEST_S3_SECRET_ACCESS_KEY")
	if endpoint == "" || bucket == "" || accessKeyID == "" || secretAccessKey == "" {
		t.Skip("set BBRAFT_TEST_S3_ENDPOINT, BBRAFT_TEST_S3_BUCKET, BBRAFT_TEST_S3_ACCESS_KEY_ID and BBRAFT_TEST_S3_SECRET_ACCESS_KEY to run against a real S3 service")
	}
	cfg := s3.Config{
		Endpoint:        endpoint,
		Region:          cmp.Or(os.Getenv("BBRAFT_TEST_S3_REGION"), "us-east-1"),
		Bucket:          bucket,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
	if v := os.Getenv("BBRAFT_TEST_S3_PATH_STYLE"); v != "" {
		pathStyle, err := strconv.ParseBool(v)
		if err != nil {
			t.Fatalf("BBRAFT_TEST_S3_PATH_STYLE is not a boolean: %q", v)
		}
		cfg.PathStyle = pathStyle
	}
	store, err := s3.New(t.Context(), cfg)
	if err != nil {
		// cfg.String redacts the secret.
		t.Fatalf("s3.New(%s): %v", cfg, err)
	}
	objstoretest.Run(t, store, objstoretest.Options{Presign: true})
}
