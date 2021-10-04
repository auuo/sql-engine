package requtil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

type StatusError struct {
	Code int
	Body []byte
}

func (e *StatusError) Error() string {
	return "status code: " + strconv.Itoa(e.Code) + ", body: " + string(e.Body)
}

func GetEntity(url string, header, params map[string]interface{}, entity interface{}) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	// build header.
	for k, v := range header {
		req.Header.Set(k, fmt.Sprintf("%v", v))
	}
	// build url query.
	if len(params) > 0 {
		q := req.URL.Query()
		for k, v := range params {
			q.Set(k, fmt.Sprintf("%v", v))
		}
		req.URL.RawQuery = q.Encode()
	}
	// do request.
	return DoRequest(req, entity)
}

func PostEntity(url string, header map[string]interface{}, body interface{}, entity interface{}) error {
	return methodEntity(http.MethodPost, url, header, body, entity)
}

func DeleteEntity(url string, header map[string]interface{}, body interface{}, entity interface{}) error {
	return methodEntity(http.MethodDelete, url, header, body, entity)
}

func DoRequest(req *http.Request, entity interface{}) error {
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return &StatusError{
			Code: resp.StatusCode,
			Body: body,
		}
	}
	if entity != nil {
		if err = json.NewDecoder(resp.Body).Decode(entity); err != nil {
			return fmt.Errorf("decoder entity error: %w", err)
		}
	}
	return nil
}

func methodEntity(method, url string, header map[string]interface{}, body interface{}, entity interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		marshal, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode body error: %w", err)
		}
		bodyReader = bytes.NewReader(marshal)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return err
	}
	// build header.
	for k, v := range header {
		req.Header.Set(k, fmt.Sprintf("%v", v))
	}
	// do request.
	return DoRequest(req, entity)
}
