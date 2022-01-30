package app

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/shipper"

	"github.com/simonswine/glowmarkt-importer/api"
)

type logLevelOverride struct {
	next  log.Logger
	level interface{}
}

func (l *logLevelOverride) Log(keyvals ...interface{}) error {
	for i := 0; i < len(keyvals); i += 2 {
		if n, ok := keyvals[0].(string); ok && n == "level" {
			keyvals[i+1] = l.level
			return l.next.Log(keyvals...)
		}
	}
	kvs := make([]interface{}, len(keyvals)+2)
	kvs[0], kvs[1] = level.Key(), l.level
	copy(kvs[2:], keyvals)
	return l.next.Log(kvs...)
}

type config struct {
	glowmarktEmail    string
	glowmarktPassword string

	tsdbPath        string
	tsdbBlockLength time.Duration
	tsdbRetention   time.Duration

	externalLabels func() labels.Labels

	thanosBucketObj []byte
}

func defaultConfig() *config {
	return &config{
		externalLabels: func() labels.Labels {
			return labels.FromStrings("cluster", "glowmarkt-importer")
		},

		tsdbPath:        "./tsdb",
		tsdbBlockLength: 2 * time.Hour,
		tsdbRetention:   365 * 24 * time.Hour,
	}
}

type App struct {
	logger log.Logger
	reg    *prometheus.Registry
	cfg    *config
}

type NewOption func(*App)

func WithLogger(l log.Logger) NewOption {
	return func(a *App) {
		a.logger = l
	}
}

func WithGlowmarktLogin(email, password string) NewOption {
	return func(a *App) {
		a.cfg.glowmarktEmail = email
		a.cfg.glowmarktPassword = password
	}
}

func WithTSDBPath(s string) NewOption {
	return func(a *App) {
		a.cfg.tsdbPath = s
	}
}

func WithTSDBBlockLength(d time.Duration) NewOption {
	return func(a *App) {
		a.cfg.tsdbBlockLength = d
	}
}

func WithTSDBRetention(d time.Duration) NewOption {
	return func(a *App) {
		a.cfg.tsdbRetention = d
	}
}

func WithExternalLabels(strs ...string) NewOption {
	return func(a *App) {
		a.cfg.externalLabels = func() labels.Labels {
			return labels.FromStrings(strs...)
		}
	}
}

func WithThanosBucketObj(str string) NewOption {
	return func(a *App) {
		a.cfg.thanosBucketObj = []byte(str)
	}
}

func New(opts ...NewOption) *App {
	a := &App{
		reg:    prometheus.NewRegistry(),
		logger: log.NewNopLogger(),
		cfg:    defaultConfig(),
	}

	for _, o := range opts {
		o(a)
	}

	return a
}

// uploadLocalTSDB uploads the local TSDB blocks generated using a thanos shipper component
func (a *App) uploadLocalTSDB(ctx context.Context) error {
	source := metadata.SourceType("importer")

	bkt, err := client.NewBucket(a.logger, a.cfg.thanosBucketObj, a.reg, string(source))
	if err != nil {
		return err
	}
	// Ensure we close up everything properly.
	defer func() {
		if err != nil {
			runutil.CloseWithLogOnErr(a.logger, bkt, "bucket client")
		}
	}()

	// upload new blocks
	s := shipper.New(
		a.logger,
		a.reg,
		a.cfg.tsdbPath,
		bkt,
		a.cfg.externalLabels,
		source,
		true,
		true,
		metadata.SHA256Func,
	)

	n, err := s.Sync(ctx)
	if err != nil {
		return err
	}

	_ = level.Info(a.logger).Log("msg", fmt.Sprintf("successfully uploaded %d blocks", n))
	return nil
}

func (app *App) newResource(ctx context.Context, c *api.Client, ar *api.Resource, q storage.Querier) (*resource, error) {
	var (
		metricName          = strings.ReplaceAll(strings.ToLower(ar.Name), " ", "_") + "_kwh"
		resourceIDLabelName = "resource_id"
	)
	lbls := labels.NewBuilder(app.cfg.externalLabels())
	lbls.Set("job", "glowmarkt-importer")
	lbls.Set(labels.MetricName, strings.ReplaceAll(strings.ToLower(ar.Name), " ", "_")+"_kwh")
	lbls.Set(resourceIDLabelName, ar.ResourceID)

	r := &resource{
		c:      c,
		id:     ar.ResourceID,
		lbls:   lbls.Labels(),
		logger: app.logger,
	}

	// find the latest reading
	result := q.Select(
		true,
		nil,
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
		labels.MustNewMatcher(labels.MatchEqual, resourceIDLabelName, ar.ResourceID),
	)

	for result.Next() {
		s := result.At().Iterator()
		var ts int64
		var value float64
		for s.Next() {
			ts, value = s.At()
		}
		r.minTime = timestamp.Time(ts).Add(api.ResourceReadingDuration).Add(-time.Second)
		r.value = value

		_ = level.Debug(r.logger).Log("msg", "found existing reading, start from there", "name", r.lbls.Get(labels.MetricName), "reading_time", timestamp.Time(ts), "value", value)
	}

	return r, r.next(ctx)
}

type resource struct {
	c      *api.Client
	id     string
	logger log.Logger

	ref  storage.SeriesRef
	lbls labels.Labels

	minTime time.Time
	maxTime time.Time

	readings []api.ResourceReading
	pos      int
	value    float64
	finished bool
}

func (r *resource) at() time.Time {
	return r.readings[r.pos].Time
}

func (r *resource) append(ctx context.Context, a storage.Appender) error {
	var err error

	_ = level.Debug(r.logger).Log("msg", "add reading", "name", r.lbls.Get(labels.MetricName), "reading_time", r.at(), "value", r.value)
	r.ref, err = a.Append(r.ref, r.lbls, timestamp.FromTime(r.at()), r.value)
	if err != nil {
		return err
	}
	return r.next(ctx)
}

// make next value available
func (r *resource) next(ctx context.Context) error {
	// return early if data is available
	if len(r.readings) > r.pos+1 {
		r.pos += 1
		r.value += r.readings[r.pos].Value
		return nil
	}

	// check if minTime is set
	if r.minTime.IsZero() {
		t, err := r.c.GetResourceFirstTime(ctx, r.id)
		if err != nil {
			return err
		}
		r.minTime = t
	}

	// check if maxTime is set
	if r.maxTime.IsZero() {
		t, err := r.c.GetResourceLastTime(ctx, r.id)
		if err != nil {
			return err
		}
		r.maxTime = t
	}

	// check if the time span is bigger than max duration
	to := r.maxTime
	if to.Sub(r.minTime) > api.ResourceReadingMaxDuration {
		to = r.minTime.Add(api.ResourceReadingMaxDuration)
	}

	// check if we are finished here
	if to.Before(r.minTime) {
		r.finished = true
		return nil
	}

	// check readings
	readings, err := r.c.GetResourceReadings(ctx, r.id, r.minTime, to)
	if err != nil {
		return err
	}
	r.readings = readings.Data
	r.pos = 0

	if len(readings.Data) == 0 {
		r.finished = true
		return nil
	}
	r.value += r.readings[r.pos].Value
	r.minTime = to.Add(api.ResourceReadingDuration)

	return nil
}

func (a *App) importConsumptionIntoLocalTSDB(ctx context.Context) error {
	// open tsdb
	options := tsdb.DefaultOptions()
	options.RetentionDuration = a.cfg.tsdbRetention.Milliseconds()

	// set retention

	options.MinBlockDuration = a.cfg.tsdbBlockLength.Milliseconds()
	options.MaxBlockDuration = a.cfg.tsdbBlockLength.Milliseconds()

	_ = level.Debug(a.logger).Log("msg", "opening TSDB", "path", a.cfg.tsdbPath, "block-duration", a.cfg.tsdbBlockLength.String(), "retention", a.cfg.tsdbRetention.String())

	db, err := tsdb.Open(a.cfg.tsdbPath, &logLevelOverride{next: a.logger, level: level.DebugValue()}, a.reg, options, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	gClient, err := api.New(a.logger, a.cfg.glowmarktEmail, a.cfg.glowmarktPassword)
	if err != nil {
		return err
	}

	apiResources, err := gClient.GetResources(ctx)
	if err != nil {
		return err
	}

	if len(apiResources) == 0 {
		return fmt.Errorf("no resources found")
	}

	var resources []*resource

	// create querier
	querier, err := db.Querier(ctx, 0, math.MaxInt64)
	if err != nil {
		return err
	}

	for _, resource := range apiResources {
		if resource.DataSourceResourceTypeInfo.Unit != "kWh" {
			_ = level.Debug(a.logger).Log("msg", "skip resource because of unknown unit", "id", resource.ResourceID, "unit", resource.DataSourceResourceTypeInfo.Unit, "type", resource.DataSourceResourceTypeInfo.Type)
			continue
		}
		_ = level.Info(a.logger).Log("msg", "found resource", "id", resource.ResourceID, "unit", resource.DataSourceResourceTypeInfo.Unit, "type", resource.DataSourceResourceTypeInfo.Type)

		if err := gClient.CatchupResource(ctx, resource.ResourceID); err != nil {
			return err
		}

		r, err := a.newResource(ctx, gClient, resource, querier)
		if err != nil {
			return err
		}
		resources = append(resources, r)
	}

	if err := querier.Close(); err != nil {
		return err
	}

	var (
		// minimum time that resources are at
		minTime time.Time
	)

appending:
	for {
		// reset vars
		minTime = time.Time{}

		for _, res := range resources {
			if res.finished {
				break appending
			}
			if minTime.IsZero() || res.at().Before(minTime) {
				minTime = res.at()
			}
		}

		// get new appender to TSDB
		a := db.Appender(ctx)

		// append matching minimum and next
		for _, res := range resources {
			if res.at().Equal(minTime) {
				if err := res.append(ctx, a); err != nil {
					return err
				}
			}
		}

		if err := a.Commit(); err != nil {
			return err
		}
	}

	return db.Compact()
}

func (a *App) Run(ctx context.Context) error {
	if err := a.importConsumptionIntoLocalTSDB(ctx); err != nil {
		return err
	}

	if err := a.uploadLocalTSDB(ctx); err != nil {
		return err
	}

	return nil
}
