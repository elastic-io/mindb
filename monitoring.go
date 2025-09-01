package mindb

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	db     *DB
	ctx    context.Context
	cancel context.CancelFunc
	mutex  sync.RWMutex

	// 监控指标
	lastMetrics     *Metrics
	alertThresholds *AlertThresholds
	alertCallbacks  []AlertCallback

	// 历史数据
	metricsHistory []MetricsSnapshot
	maxHistorySize int
}

type AlertThresholds struct {
	MaxErrorRate     float64       // 最大错误率 (%)
	MaxAvgLatency    time.Duration // 最大平均延迟
	MaxDiskUsage     float64       // 最大磁盘使用率 (%)
	MaxMemoryUsage   int64         // 最大内存使用 (bytes)
	MaxConcurrentOps int64         // 最大并发操作数
}

type AlertCallback func(alert Alert)

type Alert struct {
	Type        string
	Severity    string
	Message     string
	Timestamp   time.Time
	MetricValue interface{}
	Threshold   interface{}
}

type MetricsSnapshot struct {
	Timestamp time.Time
	Metrics   Metrics
	DiskUsage DiskUsageInfo
}

type DiskUsageInfo struct {
	Total     uint64
	Free      uint64
	Used      uint64
	UsageRate float64
}

// NewPerformanceMonitor 创建性能监控器
func NewPerformanceMonitor(db *DB) *PerformanceMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &PerformanceMonitor{
		db:             db,
		ctx:            ctx,
		cancel:         cancel,
		maxHistorySize: 1440, // 24小时的分钟数据
		alertThresholds: &AlertThresholds{
			MaxErrorRate:     5.0,                    // 5%
			MaxAvgLatency:    100 * time.Millisecond, // 100ms
			MaxDiskUsage:     90.0,                   // 90%
			MaxMemoryUsage:   1 * 1024 * 1024 * 1024, // 1GB
			MaxConcurrentOps: 1000,                   // 1000个并发操作
		},
	}
}

// Start 启动监控
func (pm *PerformanceMonitor) Start(interval time.Duration) {
	go pm.monitorLoop(interval)
}

// Stop 停止监控
func (pm *PerformanceMonitor) Stop() {
	pm.cancel()
}

// AddAlertCallback 添加告警回调
func (pm *PerformanceMonitor) AddAlertCallback(callback AlertCallback) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.alertCallbacks = append(pm.alertCallbacks, callback)
}

// SetAlertThresholds 设置告警阈值
func (pm *PerformanceMonitor) SetAlertThresholds(thresholds *AlertThresholds) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	pm.alertThresholds = thresholds
}

// 监控循环
func (pm *PerformanceMonitor) monitorLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.collectAndAnalyzeMetrics()
		case <-pm.ctx.Done():
			return
		}
	}
}

// 收集和分析指标
func (pm *PerformanceMonitor) collectAndAnalyzeMetrics() {
	// 收集当前指标
	currentMetrics := pm.db.GetMetrics()

	// 收集磁盘使用情况
	total, free, used, err := pm.db.GetDiskUsage()
	if err != nil {
		log.Println("Failed to get disk usage: ", err)
		return
	}

	diskUsage := DiskUsageInfo{
		Total:     total,
		Free:      free,
		Used:      used,
		UsageRate: float64(used) / float64(total) * 100,
	}

	// 创建快照
	snapshot := MetricsSnapshot{
		Timestamp: time.Now(),
		Metrics:   *currentMetrics,
		DiskUsage: diskUsage,
	}

	// 保存历史数据
	pm.saveMetricsSnapshot(snapshot)

	// 检查告警条件
	pm.checkAlerts(snapshot)

	// 更新最后指标
	pm.mutex.Lock()
	pm.lastMetrics = currentMetrics
	pm.mutex.Unlock()
}

// 保存指标快照
func (pm *PerformanceMonitor) saveMetricsSnapshot(snapshot MetricsSnapshot) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.metricsHistory = append(pm.metricsHistory, snapshot)

	// 限制历史数据大小
	if len(pm.metricsHistory) > pm.maxHistorySize {
		pm.metricsHistory = pm.metricsHistory[1:]
	}
}

// 检查告警条件
func (pm *PerformanceMonitor) checkAlerts(snapshot MetricsSnapshot) {
	pm.mutex.RLock()
	thresholds := pm.alertThresholds
	callbacks := pm.alertCallbacks
	pm.mutex.RUnlock()

	if thresholds == nil {
		return
	}

	// 检查错误率
	totalOps := snapshot.Metrics.ReadOps + snapshot.Metrics.WriteOps +
		snapshot.Metrics.DeleteOps + snapshot.Metrics.ListOps
	if totalOps > 0 {
		errorRate := float64(snapshot.Metrics.ErrorCount) / float64(totalOps) * 100
		if errorRate > thresholds.MaxErrorRate {
			alert := Alert{
				Type:        "ErrorRate",
				Severity:    "High",
				Message:     fmt.Sprintf("Error rate %.2f%% exceeds threshold %.2f%%", errorRate, thresholds.MaxErrorRate),
				Timestamp:   snapshot.Timestamp,
				MetricValue: errorRate,
				Threshold:   thresholds.MaxErrorRate,
			}
			pm.triggerAlert(alert, callbacks)
		}
	}

	// 检查平均延迟
	avgLatency := time.Duration((snapshot.Metrics.AvgReadLatency + snapshot.Metrics.AvgWriteLatency) / 2)
	if avgLatency > thresholds.MaxAvgLatency {
		alert := Alert{
			Type:        "Latency",
			Severity:    "Medium",
			Message:     fmt.Sprintf("Average latency %v exceeds threshold %v", avgLatency, thresholds.MaxAvgLatency),
			Timestamp:   snapshot.Timestamp,
			MetricValue: avgLatency,
			Threshold:   thresholds.MaxAvgLatency,
		}
		pm.triggerAlert(alert, callbacks)
	}

	// 检查磁盘使用率
	if snapshot.DiskUsage.UsageRate > thresholds.MaxDiskUsage {
		alert := Alert{
			Type:        "DiskUsage",
			Severity:    "High",
			Message:     fmt.Sprintf("Disk usage %.2f%% exceeds threshold %.2f%%", snapshot.DiskUsage.UsageRate, thresholds.MaxDiskUsage),
			Timestamp:   snapshot.Timestamp,
			MetricValue: snapshot.DiskUsage.UsageRate,
			Threshold:   thresholds.MaxDiskUsage,
		}
		pm.triggerAlert(alert, callbacks)
	}

	// 检查并发操作数
	concurrentOps := snapshot.Metrics.ActiveReads + snapshot.Metrics.ActiveWrites
	if concurrentOps > thresholds.MaxConcurrentOps {
		alert := Alert{
			Type:        "ConcurrentOps",
			Severity:    "Medium",
			Message:     fmt.Sprintf("Concurrent operations %d exceeds threshold %d", concurrentOps, thresholds.MaxConcurrentOps),
			Timestamp:   snapshot.Timestamp,
			MetricValue: concurrentOps,
			Threshold:   thresholds.MaxConcurrentOps,
		}
		pm.triggerAlert(alert, callbacks)
	}
}

// 触发告警
func (pm *PerformanceMonitor) triggerAlert(alert Alert, callbacks []AlertCallback) {
	log.Printf("Performance Alert: %s - %s\n", alert.Type, alert.Message)

	for _, callback := range callbacks {
		go func(cb AlertCallback) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Alert callback panic: %v\n", r)
				}
			}()
			cb(alert)
		}(callback)
	}
}

// GetMetricsHistory 获取历史指标
func (pm *PerformanceMonitor) GetMetricsHistory(duration time.Duration) []MetricsSnapshot {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	cutoff := time.Now().Add(-duration)
	var result []MetricsSnapshot

	for _, snapshot := range pm.metricsHistory {
		if snapshot.Timestamp.After(cutoff) {
			result = append(result, snapshot)
		}
	}

	return result
}

// GetCurrentMetrics 获取当前指标
func (pm *PerformanceMonitor) GetCurrentMetrics() *Metrics {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	if pm.lastMetrics != nil {
		metrics := *pm.lastMetrics
		return &metrics
	}

	return pm.db.GetMetrics()
}

// GenerateReport 生成性能报告
func (pm *PerformanceMonitor) GenerateReport(duration time.Duration) *PerformanceReport {
	history := pm.GetMetricsHistory(duration)
	if len(history) == 0 {
		return nil
	}

	report := &PerformanceReport{
		StartTime: history[0].Timestamp,
		EndTime:   history[len(history)-1].Timestamp,
		Duration:  duration,
	}

	// 计算统计数据
	var (
		totalOps     int64
		totalErrors  int64
		totalLatency int64
		maxLatency   int64
		minLatency   int64 = 999999999999 // 初始化为很大的值
		maxDiskUsage float64
		samples      int64
	)

	for _, snapshot := range history {
		ops := snapshot.Metrics.ReadOps + snapshot.Metrics.WriteOps +
			snapshot.Metrics.DeleteOps + snapshot.Metrics.ListOps
		totalOps += ops
		totalErrors += snapshot.Metrics.ErrorCount

		avgLatency := (snapshot.Metrics.AvgReadLatency + snapshot.Metrics.AvgWriteLatency) / 2
		totalLatency += avgLatency

		if avgLatency > maxLatency {
			maxLatency = avgLatency
		}
		if avgLatency < minLatency {
			minLatency = avgLatency
		}

		if snapshot.DiskUsage.UsageRate > maxDiskUsage {
			maxDiskUsage = snapshot.DiskUsage.UsageRate
		}

		samples++
	}

	if samples > 0 {
		report.TotalOperations = totalOps
		report.TotalErrors = totalErrors
		report.ErrorRate = float64(totalErrors) / float64(totalOps) * 100
		report.AvgLatency = time.Duration(totalLatency / samples)
		report.MaxLatency = time.Duration(maxLatency)
		report.MinLatency = time.Duration(minLatency)
		report.MaxDiskUsage = maxDiskUsage
		report.OperationsPerSecond = float64(totalOps) / duration.Seconds()
	}

	return report
}

type PerformanceReport struct {
	StartTime           time.Time
	EndTime             time.Time
	Duration            time.Duration
	TotalOperations     int64
	TotalErrors         int64
	ErrorRate           float64
	AvgLatency          time.Duration
	MaxLatency          time.Duration
	MinLatency          time.Duration
	MaxDiskUsage        float64
	OperationsPerSecond float64
}

// String 格式化报告输出
func (pr *PerformanceReport) String() string {
	return fmt.Sprintf(`Performance Report (%v - %v)
Duration: %v
Total Operations: %d
Total Errors: %d
Error Rate: %.2f%%
Average Latency: %v
Max Latency: %v
Min Latency: %v
Max Disk Usage: %.2f%%
Operations/Second: %.2f`,
		pr.StartTime.Format("2006-01-02 15:04:05"),
		pr.EndTime.Format("2006-01-02 15:04:05"),
		pr.Duration,
		pr.TotalOperations,
		pr.TotalErrors,
		pr.ErrorRate,
		pr.AvgLatency,
		pr.MaxLatency,
		pr.MinLatency,
		pr.MaxDiskUsage,
		pr.OperationsPerSecond)
}
