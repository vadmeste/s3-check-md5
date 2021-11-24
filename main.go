package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	endpoint, accessKey, secretKey string
	bucket, prefix                 string
)

func main() {

	flag.StringVar(&endpoint, "endpoint", "https://play.min.io", "S3 endpoint URL")
	flag.StringVar(&accessKey, "access-key", "Q3AM3UQ867SPQQA43P2F", "S3 Access Key")
	flag.StringVar(&secretKey, "secret-key", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", "S3 Secret Key")
	flag.StringVar(&bucket, "bucket", "", "Select a specific bucket")
	flag.StringVar(&prefix, "prefix", "", "Select a prefix")
	flag.Parse()

	if endpoint == "" {
		log.Fatalln("Endpoint is not provided")
	}

	if accessKey == "" {
		log.Fatalln("Access key is not provided")
	}

	if secretKey == "" {
		log.Fatalln("Secret key is not provided")
	}

	if bucket == "" && prefix != "" {
		log.Fatalln("--prefix is specified without --bucket.")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: strings.EqualFold(u.Scheme, "https"),
	})
	if err != nil {
		log.Fatalln()
	}

	// s3Client.TraceOn(os.Stderr)

	var buckets []string
	if bucket != "" {
		buckets = append(buckets, bucket)
	} else {
		bucketsInfo, err := s3Client.ListBuckets(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		for _, b := range bucketsInfo {
			buckets = append(buckets, b.Name)
		}
	}

	for _, bucket := range buckets {
		opts := minio.ListObjectsOptions{
			Recursive:    true,
			Prefix:       prefix,
			WithVersions: true,
		}

		// List all objects from a bucket-name with a matching prefix.
		for object := range s3Client.ListObjects(context.Background(), bucket, opts) {
			if object.Err != nil {
				fmt.Println("LIST error:", object.Err)
				continue
			}
			if object.IsDeleteMarker {
				continue
			}
			if strings.Contains(object.ETag, "-") {
				log.Println("IGNORING", bucket, object.Key, object.VersionID, "with etag =", object.ETag)
				continue
			}
			obj, err := s3Client.GetObject(context.Background(), bucket, object.Key,
				minio.GetObjectOptions{VersionID: object.VersionID})
			if err != nil {
				log.Println("GET", bucket, object.Key, object.VersionID, "=>", err)
				continue
			}
			h := md5.New()
			if _, err := io.Copy(h, obj); err != nil {
				log.Println("MD5 calculation error:", bucket, object.Key, object.VersionID, "=>", err)
				continue
			}
			md5sum := fmt.Sprintf("%x", h.Sum(nil))
			if md5sum != object.ETag {
				fmt.Println("CORRUPTED object:", bucket, object.Key, object.VersionID)
			} else {
				fmt.Println("INTACT", bucket, object.Key, object.VersionID)
			}
		}
	}

	return
}
