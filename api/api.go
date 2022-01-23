package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const (
	applicationID = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
	baseURL       = "https://api.glowmarkt.com/api/v0-1"
	authURL       = baseURL + "/auth"
	resourcesURL  = baseURL + "/resource"
)

type additionalHeaders struct {
	http.Header
	token func() (string, error)
}

func (a *additionalHeaders) RoundTrip(req *http.Request) (*http.Response, error) {
	for key, val := range a.Header {
		req.Header[key] = val
	}

	if requestRequiresToken(req.Context()) {
		t, err := a.token()
		if err != nil {
			return nil, err
		}
		req.Header.Set("Token", t)
	}

	return http.DefaultTransport.RoundTrip(req)
}

func New(logger log.Logger, email, password string) (*Client, error) {
	var h = additionalHeaders{
		Header: make(http.Header),
	}
	h.Set("applicationID", applicationID)
	h.Set("content-type", "application/json")

	c := &Client{
		logger: logger,
		httpClient: &http.Client{
			Transport: &h,
		},

		email:    email,
		password: password,
	}

	h.token = func() (string, error) {
		c.tokenMtx.Lock()
		defer c.tokenMtx.Unlock()

		if c.token == nil || c.token.expires.Before(time.Now()) {
			t, err := c.login()
			if err != nil {
				return "", err
			}
			c.token = t
		}

		return c.token.token, nil
	}

	return c, nil
}

type token struct {
	expires time.Time
	token   string
}

type Client struct {
	httpClient *http.Client
	logger     log.Logger

	email    string
	password string

	token    *token
	tokenMtx sync.Mutex
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c *Client) login() (*token, error) {
	reqData, err := json.Marshal(&loginRequest{
		Username: c.email,
		Password: c.password,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", authURL, bytes.NewBuffer(reqData))
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		_ = level.Debug(c.logger).Log("msg", "trying to login", "body", string(b), "req", reqData)
		return nil, fmt.Errorf("unexpected status code, when logging in: %d", resp.StatusCode)
	}

	var respData struct {
		Token     string `json:"token"`
		Valid     bool   `json:"valid"`
		AccountID string `json:"accountId"`
		Name      string `json:"name"`
		Exp       int    `json:"exp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return nil, err
	}
	_ = level.Debug(c.logger).Log("msg", "trying to login")

	if !respData.Valid {
		return nil, errors.New("login at glowmarkt api failed")
	}
	expires := time.Unix(int64(respData.Exp), 0)

	_ = level.Debug(c.logger).Log("msg", "successfully logged into glowmarkt API", "accountID", respData.AccountID, "expires", time.Until(expires))

	return &token{
		expires: expires,
		token:   respData.Token,
	}, nil

}

type contextKey uint8

const (
	contextKeyRequestRequiresToken contextKey = iota
)

func withRequestRequiresToken(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyRequestRequiresToken, true)
}

func requestRequiresToken(ctx context.Context) bool {
	v, ok := ctx.Value(contextKeyRequestRequiresToken).(bool)
	if ok {
		return v
	}
	return false
}

type Resource struct {
	Active                     bool   `json:"active"`
	ResourceTypeID             string `json:"resourceTypeId"`
	OwnerID                    string `json:"ownerId"`
	Name                       string `json:"name"`
	Description                string `json:"description"`
	Label                      string `json:"label"`
	DataSourceResourceTypeInfo struct {
		Unit string `json:"unit"`
		Type string `json:"type"`
	} `json:"dataSourceResourceTypeInfo"`
	DataSourceType     string    `json:"dataSourceType"`
	Classifier         string    `json:"classifier"`
	BaseUnit           string    `json:"baseUnit"`
	ResourceID         string    `json:"resourceId"`
	UpdatedAt          time.Time `json:"updatedAt"`
	CreatedAt          time.Time `json:"createdAt"`
	DataSourceUnitInfo struct {
		Shid string `json:"shid"`
	} `json:"dataSourceUnitInfo"`
	HasOwnerPermissions bool `json:"hasOwnerPermissions"`
}

func (c *Client) GetResources(ctx context.Context) ([]*Resource, error) {
	var resources []*Resource

	req, err := http.NewRequestWithContext(withRequestRequiresToken(ctx), "GET", resourcesURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(resp.Body).Decode(&resources); err != nil {
		return nil, err
	}

	return resources, nil
}

func (c *Client) CatchupResource(ctx context.Context, resourceID string) error {
	req, err := http.NewRequestWithContext(withRequestRequiresToken(ctx), "GET", resourceURL(resourceID)+"/catchup", nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var body struct {
		Data struct {
			Valid  bool   `json:"valid"`
			Status string `json:"status"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return err
	}

	_ = level.Debug(c.logger).Log("msg", "request to catchup with latest data", "status", body.Data.Status)

	return nil
}

const timeFormat = "2006-01-02T15:04:05"

func resourceURL(id string) string {
	return resourcesURL + "/" + url.PathEscape(id)
}

func (c *Client) GetResourceFirstTime(ctx context.Context, resourceID string) (time.Time, error) {
	req, err := http.NewRequestWithContext(withRequestRequiresToken(ctx), "GET", resourceURL(resourceID)+"/first-time", nil)
	if err != nil {
		return time.Time{}, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}

	var result struct {
		Data struct {
			FirstTs int64 `json:"firstTs"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, err
	}

	ts := time.Unix(result.Data.FirstTs, 0)
	_ = level.Debug(c.logger).Log("msg", "get first time", "first-time", ts)

	return ts, nil
}

func (c *Client) GetResourceLastTime(ctx context.Context, resourceID string) (time.Time, error) {
	req, err := http.NewRequestWithContext(withRequestRequiresToken(ctx), "GET", resourceURL(resourceID)+"/last-time", nil)
	if err != nil {
		return time.Time{}, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}

	var result struct {
		Data struct {
			LastTs int64 `json:"lastTs"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, err
	}

	ts := time.Unix(result.Data.LastTs, 0)
	_ = level.Debug(c.logger).Log("msg", "get last time", "last-time", ts)

	return ts, nil
}

type ResourceReadings struct {
	Status         string `json:"status"`
	Name           string `json:"name"`
	ResourceTypeID string `json:"resourceTypeId"`
	ResourceID     string `json:"resourceId"`
	Query          struct {
		From     string `json:"from"`
		To       string `json:"to"`
		Period   string `json:"period"`
		Function string `json:"function"`
	} `json:"query"`
	Data       []ResourceReading `json:"data"`
	Units      string            `json:"units"`
	Classifier string            `json:"classifier"`
}

type ResourceReading struct {
	Time  time.Time
	Value float64
}

func (r *ResourceReading) UnmarshalJSON(b []byte) error {
	var values []float64
	if err := json.Unmarshal(b, &values); err != nil {
		return err
	}
	if len(values) != 2 {
		return fmt.Errorf("expected exactly to array elements in '%s'", string(b))
	}

	r.Time = time.Unix(int64(values[0]), 0)
	r.Value = values[1]

	return nil
}

const ResourceReadingMaxDuration = time.Hour * 24 * 10
const ResourceReadingDuration = time.Minute * 30

func (c *Client) GetResourceReadings(ctx context.Context, resourceID string, from, to time.Time) (*ResourceReadings, error) {
	if diff := to.Sub(from); diff < 0 {
		return nil, fmt.Errorf("from needs to be smaller then to")
	} else if diff > ResourceReadingMaxDuration {
		return nil, fmt.Errorf("time duration is bigger than the maximum duration")
	}

	u, err := url.Parse(resourceURL(resourceID) + "/readings")
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("nulls", "1")
	q.Set("function", "sum") // also valid max,min,avg
	q.Set("period", "PT30M")
	q.Set("from", from.Format(timeFormat))
	q.Set("to", to.Format(timeFormat))
	u.RawQuery = q.Encode()

	_ = level.Debug(c.logger).Log("msg", "download readings", "url", u.String())

	req, err := http.NewRequestWithContext(withRequestRequiresToken(ctx), "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	var readings ResourceReadings

	if err := json.NewDecoder(resp.Body).Decode(&readings); err != nil {
		return nil, err
	}

	return &readings, nil
}
