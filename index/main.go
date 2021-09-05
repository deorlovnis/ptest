package main

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
	"github.com/valyala/fastjson"
)

// hardcoded because there is no case in this task that it might be different
// could be requested via command line args to make it more of a real tool
const (
	PDBASEURL = "https://testcomp3.pipedrive.com"
	BUCKET    = "pdw-export.zulu"
	ITEM      = "test_tasks/deals.csv.gz"
	REGION    = "eu-central-1"
)

type Deal struct {
	Title    string  `json:"title,omitempty"`
	Currency string  `json:"currency,omitempty"`
	Value    float64 `json:"value,omitempty"`
	Status   string  `json:"status,omitempty"`
}

func main() {
	prepEnv()

	buf := aws.NewWriteAtBuffer([]byte{})

	getS3Data(buf)

	dataSCV := prepCSV(buf)

	dealsAWS := make(map[string]Deal)

	// simplified parses that assumes data is correct
	// making pointer to maps isn't strightforward in go
	// decided to leave it as is and not spend extra time on Sunday evening
	for _, row := range dataSCV[1:] {
		value, err := strconv.ParseFloat(string(row[2]), 8)
		if err != nil {
			exitErrorf("Unable to  parse csv row, %v", err)
		}
		d := Deal{
			Title:    string(row[0]),
			Currency: string(row[1]),
			Value:    value * 2,
			Status:   string(row[3]),
		}

		dealsAWS[d.Title] = d
	}

	bodyBytes := getPipeData()

	dataJSON := getJSONdata(bodyBytes)

	dealsPiped := make(map[string]Deal)

	for _, v := range dataJSON {
		val := v.GetFloat64("value")
		tit := v.GetStringBytes("title")

		d := Deal{
			Value: val,
			Title: string(tit),
		}

		dealsPiped[d.Title] = d
	}

	// compare and update data
	for _, v := range dealsAWS {
		if val, ok := dealsPiped[v.Title]; ok {
			// update pipidrive value with AWS value
			if val.Value != v.Value {
				val.Value = v.Value
			}
		} else {
			dealsPiped[v.Title] = v
		}
	}

	updateAPIdata(&dealsPiped)
}

func prepEnv() {
	err := godotenv.Load()

	if err != nil {
		exitErrorf("Unable to load .env, %v", err)
	}
}

// to make errors a bit more readable
// and making sure that the program stops
func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)

	os.Exit(1)
}

func getS3Data(buf *aws.WriteAtBuffer) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(REGION),
		Credentials: credentials.NewEnvCredentials(),
	})

	if err != nil {
		exitErrorf("Unable to initialize creds, %v", err)
	}

	downloader := s3manager.NewDownloader(sess)

	_, err = downloader.Download(buf,
		&s3.GetObjectInput{
			Bucket: aws.String(BUCKET),
			Key:    aws.String(ITEM),
		})

	if err != nil {
		exitErrorf("Unable to download item %q, %v", ITEM, err)
	}
}

func prepCSV(buf *aws.WriteAtBuffer) [][]string {
	gr, err := gzip.NewReader(bytes.NewBuffer(buf.Bytes()))

	if err != nil {
		exitErrorf("Unadble to read gzip, %v", err)
	}

	defer gr.Close()

	cr := csv.NewReader(gr)

	dataCSV, err := cr.ReadAll()

	if err != nil {
		exitErrorf("Unadble to read csv, %v", err)
	}

	return dataCSV
}

func getPipeData() []byte {
	resp, err := http.Get(PDBASEURL + "/api/v1/deals:(title,value)?api_token=" + os.Getenv("PIPED_TOKEN"))

	if err != nil {
		exitErrorf("Unable to dowdload data from pipedrive API, %v", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		exitErrorf("Unable to read response body, %v", err)
	}

	return body
}

func getJSONdata(bodyBytes []byte) []*fastjson.Value {
	var JSONparser fastjson.Parser

	v, err := JSONparser.ParseBytes(bodyBytes)

	if err != nil {
		exitErrorf("Unable to parse json, %v", err)
	}

	data := v.GetArray("data")

	return data
}

func updateAPIdata(deals *map[string]Deal) {
	postURL := PDBASEURL + "/api/v1/deals?api_token=" + os.Getenv("PIPED_TOKEN")

	client := &http.Client{}

	i := 0

	for _, v := range *deals {
		// assuming token enables 20 request per 2 seconds
		if i%20 == 0 {
			time.Sleep(time.Second * 2)
		}

		j, err := json.Marshal(v)

		if err != nil {
			exitErrorf("Unable to marshal a deal to JSON, %v", err)
		}

		go postDeal(postURL, client, j)

		i++
	}
}

func postDeal(postURL string, client *http.Client, data []byte) {
	req, err := http.NewRequest("POST", postURL, bytes.NewBuffer(data))

	if err != nil {
		exitErrorf("Unable to create a new request, %v", err)

	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		exitErrorf("Unable to request API, %v", err)
	}

	defer resp.Body.Close()

	fmt.Println("status:", resp.Status, '\n')
}
