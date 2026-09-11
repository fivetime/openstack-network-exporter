// SPDX-License-Identifier: Apache-2.0

package log

import "github.com/prometheus/client_golang/prometheus"

// ErrorsTotal counts error and critical messages whatever the log level, so
// that a failing collector shows up in the metrics and not only in the logs.
var ErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "openstack_network_exporter_errors_total",
	Help: "Number of error and critical messages logged by the exporter, for example failed collector calls.",
})
