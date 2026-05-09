#!/usr/bin/env bash
set -Eeuo pipefail

: "${NBD_TEST_S3_ENDPOINT_URL:?missing NBD_TEST_S3_ENDPOINT_URL}"
: "${NBD_TEST_S3_BUCKET:?missing NBD_TEST_S3_BUCKET}"
: "${NBD_TEST_S3_ACCESS_KEY_ID:?missing NBD_TEST_S3_ACCESS_KEY_ID}"
: "${NBD_TEST_S3_SECRET_ACCESS_KEY:?missing NBD_TEST_S3_SECRET_ACCESS_KEY}"
: "${NBD_TEST_S3_REGION:?missing NBD_TEST_S3_REGION}"
: "${NBD_TEST_S3_KEY_PREFIX:?missing NBD_TEST_S3_KEY_PREFIX}"

cargo test -p nbd-server --features s3 --lib storage::s3
cargo test -p nbd-server --features s3 --test s3_blob_store
