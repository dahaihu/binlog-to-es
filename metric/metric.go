package metric

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	IndexCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "created_index",
		Help: "created_doc_count",
	})
	IndexUpdated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "updated_index",
		Help: "updated_doc_count",
	})
	IndexDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "deleted_index",
		Help: "deleted_doc_count",
	})
	Queued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "queued",
		Help: "queued_count",
	})
	FlushTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "flush_time",
		Help: "es_flush_time",
	})
	SyncGap = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mysql_gap",
		Help: "mysql_gap",
	})
)

func init() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe(":2112", nil)
	}()
}
