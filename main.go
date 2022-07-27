package main

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/linkedin/goavro/v2"
)

const loginEventAvroSchema = `{"type": "record", "name": "LoginEvent", "fields": [{"name": "Username", "type": "string"}]}`

func main() {
	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}
	m := map[string]interface{}{
		"Username": "Kamol Hasan",
	}
	native, err := codec.BinaryFromNative(nil, m)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(native))

	// Configure to use MinIO Server
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("ZqMh3MoLbPK8rm6h", "gUjvGT4BGMW1gbV0rcoxTEZbglUw4bdd", ""),
		Endpoint:         aws.String("http://172.17.0.2:9000"),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(string(native)),
		Bucket: aws.String("newbucket"),
		Key:    aws.String("login.avro"),
	})
	if err != nil {
		panic(err)
	}

	out, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("newbucket"),
		Key:    aws.String("login.avro"),
	})
	if err != nil {
		panic(err)
	}

	resp, err := ioutil.ReadAll(out.Body)
	if err != nil {
		panic(err)
	}

	mp, _, err := codec.NativeFromBinary(resp)
	if err != nil {
		panic(err)
	}

	byt, err := codec.TextualFromNative(nil, mp)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(byt))

}
