package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

type Registry struct {
	mu         sync.Mutex
	counters   map[string]float64
	histograms map[string]*Histogram
}

type Histogram struct {
	Buckets []float64
	Counts  []float64
	Sum     float64
	Total   float64
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   map[string]float64{},
		histograms: map[string]*Histogram{},
	}
}

func (r *Registry) IncCounter(name string, labels map[string]string, delta float64) {
	key := metricKey(name, labels)
	r.mu.Lock()
	r.counters[key] += delta
	r.mu.Unlock()
}

func (r *Registry) SetGauge(name string, labels map[string]string, v float64) {
	key := metricKey(name, labels)
	r.mu.Lock()
	r.counters[key] = v
	r.mu.Unlock()
}

func (r *Registry) ObserveHistogram(name string, labels map[string]string, value float64, buckets []float64) {
	key := metricKey(name, labels)
	r.mu.Lock()
	h, ok := r.histograms[key]
	if !ok {
		b := append([]float64(nil), buckets...)
		sort.Float64s(b)
		h = &Histogram{
			Buckets: b,
			Counts:  make([]float64, len(b)),
		}
		r.histograms[key] = h
	}
	h.Total += 1
	h.Sum += value
	for i, ub := range h.Buckets {
		if value <= ub {
			h.Counts[i] += 1
		}
	}
	r.mu.Unlock()
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.mu.Lock()
		defer r.mu.Unlock()

		keys := make([]string, 0, len(r.counters))
		for k := range r.counters {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			_, _ = fmt.Fprintf(w, "%s %g\n", k, r.counters[k])
		}

		hkeys := make([]string, 0, len(r.histograms))
		for k := range r.histograms {
			hkeys = append(hkeys, k)
		}
		sort.Strings(hkeys)
		for _, k := range hkeys {
			h := r.histograms[k]
			base, labels := splitMetricKey(k)
			for i, ub := range h.Buckets {
				metric := base + "_bucket"
				lbl := mergeLabels(labels, map[string]string{"le": trimFloat(ub)})
				_, _ = fmt.Fprintf(w, "%s%s %g\n", metric, formatLabels(lbl), h.Counts[i])
			}
			metric := base + "_bucket"
			lbl := mergeLabels(labels, map[string]string{"le": "+Inf"})
			_, _ = fmt.Fprintf(w, "%s%s %g\n", metric, formatLabels(lbl), h.Total)
			_, _ = fmt.Fprintf(w, "%s_sum%s %g\n", base, formatLabels(labels), h.Sum)
			_, _ = fmt.Fprintf(w, "%s_count%s %g\n", base, formatLabels(labels), h.Total)
		}
	})
}

func metricKey(name string, labels map[string]string) string {
	return name + formatLabels(labels)
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, escapeLabelValue(labels[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func splitMetricKey(key string) (string, map[string]string) {
	i := strings.IndexByte(key, '{')
	if i == -1 {
		return key, map[string]string{}
	}
	return key[:i], parseLabels(key[i:])
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	if strings.TrimSpace(s) == "" {
		return out
	}
	for _, part := range strings.Split(s, ",") {
		p := strings.SplitN(part, "=", 2)
		if len(p) != 2 {
			continue
		}
		k := p[0]
		v := strings.Trim(p[1], `"`)
		out[k] = strings.ReplaceAll(strings.ReplaceAll(v, `\"`, `"`), `\\`, `\`)
	}
	return out
}

func mergeLabels(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func trimFloat(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", v), "0"), ".")
}

func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}
