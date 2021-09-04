package main

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
	"github.com/valyala/fastjson"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// hardcoded because there is no case in this task that it might be different
// could be requested via command line args to make it more of a real tool
const (
	pipedBaseURL = "https://testcomp3.pipedrive.com"
	bucket       = "pdw-export.zulu"
	item         = "test_tasks/deals.csv.gz"
)

type Deal struct {
	Title    string  `json:"title,omitempty"`
	Currency string  `json:"currency,omitempty"`
	Value    float64 `json:"value,omitempty"`
	Status   string  `json:"status,omitempty"`
}

func main() {
	// prepare environment
	err := godotenv.Load()

	if err != nil {
		exitErrorf("Unable to load .env, %v", err)
	}

	// get data from aws
	sess, err := session.NewSession(&aws.Config{
		// the region is hardcoded for the same reason as bucket and item
		Region:      aws.String("eu-central-1"),
		Credentials: credentials.NewEnvCredentials(),
	})

	// used this to look up for correct region.
	// region, err := s3manager.GetBucketRegion(context.TODO(), sess, bucket, "us-west-2")
	// fmt.Print(region)

	if err != nil {
		exitErrorf("Unable to initialize creds, %v", err)
	}

	downloader := s3manager.NewDownloader(sess)

	buf := aws.NewWriteAtBuffer([]byte{})

	_, err = downloader.Download(buf,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", item, err)
	}

	// unzip dataset and parse it to cash
	// parsing to cash is also a sacrifice for the sake of this test
	// in case something crashes before data was successfully sent
	// the whole process would need to be repeated
	// probably a better way would be to save data in chunks or send it in chunks
	gr, err := gzip.NewReader(bytes.NewBuffer(buf.Bytes()))

	if err != nil {
		exitErrorf("unadble to read gzip, %v", err)
	}
	defer gr.Close()

	cr := csv.NewReader(gr)
	rec, err := cr.ReadAll()
	if err != nil {
		exitErrorf("unadble to read csv, %v", err)
	}

	var dealsAWS []Deal

	// simplified parses that assumes data is correct
	for _, row := range rec[1:] {
		value, err := strconv.ParseFloat(string(row[2]), 8)
		if err != nil {
			exitErrorf("unable to  parse csv row, %v", err)
		}
		d := Deal{
			Title:    string(row[0]),
			Currency: string(row[1]),
			Value:    value * 2,
			Status:   string(row[3]),
		}

		dealsAWS = append(dealsAWS, d)
	}

	fmt.Println(dealsAWS[:10], '\n')

	// get and process deals from pipedrive api
	resp, err := http.Get(pipedBaseURL + "/api/v1/deals:(title,value)?api_token=" + os.Getenv("PIPED_TOKEN"))

	if err != nil {
		exitErrorf("unable to dowdload data from pipedrive API, %v", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		exitErrorf("unable to read response body, %v", err)
	}

	var JSONparser fastjson.Parser

	v, err := JSONparser.ParseBytes(body)

	if err != nil {
		exitErrorf("unable to parse json, %v", err)
	}

	data := v.GetArray("data")

	var dealsPiped []Deal

	for _, v := range data {
		val := v.GetFloat64("value")
		tit := v.GetStringBytes("title")

		d := Deal{
			Value: val,
			Title: string(tit),
		}

		dealsPiped = append(dealsPiped, d)
	}

	// compare values

	fmt.Print(dealsPiped)

	// want to use map to get only certain fields and avoid writing the whole structure for a deal
	// send it as is because in the email estimated finishing the task by the end of Friday
	// TODO:
	//		1. compare data from pipedrive api and s3; update api in case of value change
	//		2. send updated data if any to pipedrive api
}
