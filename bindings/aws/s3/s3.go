// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	aws_auth "github.com/dapr/components-contrib/authentication/aws"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/google/uuid"
)

// AWSS3 is a binding for an AWS S3 storage bucket
type AWSS3 struct {
	metadata   *s3Metadata
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	logger     logger.Logger
}

type s3Metadata struct {
	Region       string `json:"region"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Bucket       string `json:"bucket"`
}

// NewAWSS3 returns a new AWSS3 instance
func NewAWSS3(logger logger.Logger) *AWSS3 {
	return &AWSS3{logger: logger}
}

// Init does metadata parsing and connection creation
func (s *AWSS3) Init(metadata bindings.Metadata) error {
	m, err := s.parseMetadata(metadata)
	if err != nil {
		return err
	}
	uploader, downloader, err := s.getClient(m)
	if err != nil {
		return err
	}
	s.metadata = m
	s.uploader = uploader
	s.downloader = downloader

	return nil
}

func (s *AWSS3) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, bindings.GetOperation}
}

func (s *AWSS3) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	key := ""
	if val, ok := req.Metadata["key"]; ok && val != "" {
		key = val
	} else {
		key = uuid.New().String()
		s.logger.Debugf("key not found. generating key %s", key)
	}

	switch req.Operation {
	case bindings.CreateOperation:
		return s.create(key, req)
	case bindings.GetOperation:
		return s.get(key, req)
	case bindings.DeleteOperation, bindings.ListOperation:
		fallthrough
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (s *AWSS3) create(key string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	r := bytes.NewReader(req.Data)
	output, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.metadata.Bucket),
		Key:    aws.String(key),
		Body:   r,
	})

	b, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("error marshalling create response for s3 upload: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (s *AWSS3) get(key string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	b := &aws.WriteAtBuffer{}
	_, err := s.downloader.DownloadWithContext(context.Background(), b, &s3.GetObjectInput{
		Bucket: aws.String(s.metadata.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, fmt.Errorf("error downloading s3 object: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: b.Bytes(),
	}, nil
}

func (s *AWSS3) parseMetadata(metadata bindings.Metadata) (*s3Metadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m s3Metadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *AWSS3) getClient(metadata *s3Metadata) (*s3manager.Uploader, *s3manager.Downloader, error) {
	sess, err := aws_auth.GetClient(metadata.AccessKey, metadata.SecretKey, metadata.SessionToken, metadata.Region, metadata.Endpoint)
	if err != nil {
		return nil, nil, err
	}

	uploader := s3manager.NewUploader(sess)
	downloader := s3manager.NewDownloader(sess)
	return uploader, downloader, nil
}
