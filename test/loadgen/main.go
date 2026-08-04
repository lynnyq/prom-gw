// loadgen 性能压测客户端: 自研 Prometheus RemoteWrite 协议压测工具。
//
// 区别于 vegeta(只压 HTTP):本工具能精确控制每请求 sample 数,模拟真实负载。
//
// 用法:
//
//	go run ./test/loadgen \
//	  --url=http://127.0.0.1:19201/api/v1/write \
//	  --token=tk_app_business_dev \
//	  --rate=1500000 \            # 总目标 samples/s
//	  --samples-per-batch=500 \   # 每个 WriteRequest 包含的 sample 数
//	  --duration=60s \
//	  --concurrency=8 \
//	  --metrics-url=http://127.0.0.1:8080/metrics
//
// 输出:
//   - 每秒打印一行: t=Ns rate=1.5M samples/s sent=X err=Y p50=...ms p99=...ms
//   - 最终一行汇总 + p50/p95/p99 延迟 + 错误率
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	var (
		url         = flag.String("url", "http://127.0.0.1:19201/api/v1/write", "RemoteWrite URL")
		token       = flag.String("token", "tk_app_business_dev", "Bearer token")
		rate        = flag.Int("rate", 100000, "目标 samples/s")
		batchSize   = flag.Int("samples-per-batch", 500, "每 WriteRequest sample 数")
		duration    = flag.Duration("duration", 30*time.Second, "总运行时长")
		concurrency = flag.Int("concurrency", 4, "并发 worker 数")
		metricsURL  = flag.String("metrics-url", "", "可选 GW metrics URL, 用于拉取 GW 侧的指标")
		seriesCount = flag.Int("series-count", 10000, "不同 series 数量,用于构造逼真负载")
		verbose     = flag.Bool("v", false, "每请求日志")
	)
	flag.Parse()

	fmt.Printf("=== prom-gw loadgen ===\n")
	fmt.Printf("url=%s rate=%d samples/s batch=%d duration=%s concurrency=%d\n",
		*url, *rate, *batchSize, *duration, *concurrency)

	// 每个 worker 每秒需发送的 batch 数
	batchesPerSec := float64(*rate) / float64(*batchSize)
	// 总 batch 数
	totalBatches := int(float64(*duration) / float64(time.Second) * batchesPerSec)
	// 启动延迟,避免第一个 batch 把 burst 拉满
	batchInterval := time.Duration(float64(time.Second) / batchesPerSec / float64(*concurrency))

	fmt.Printf("total_batches=%d batch_interval=%s (per worker)\n", totalBatches, batchInterval)

	// 预生成 series(确定性 seed,便于复现)
	rng := rand.New(rand.NewSource(42))
	// 每 batch 的 series 数:固定 10,避免 payload 过大(每个 WriteRequest 控制在 <100KB)
	const seriesPerBatch = 10
	// 每个 series 含多少 sample = batchSize / seriesPerBatch
	if *batchSize < seriesPerBatch {
		log.Fatalf("--samples-per-batch=%d too small, need >= %d", *batchSize, seriesPerBatch)
	}
	samplesPerSeries := *batchSize / seriesPerBatch
	// series 池(远大于 seriesPerBatch,模拟真实 series 滚动)
	seriesPool := make([]prompb.TimeSeries, 0, *seriesCount)
	for i := 0; i < *seriesCount; i++ {
		s := prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)},
				{Name: "instance", Value: fmt.Sprintf("host-%d.%d.example.com", rng.Intn(100), rng.Intn(1000))},
				{Name: "job", Value: []string{"node", "app", "db", "kafka", "redis"}[rng.Intn(5)]},
			},
			Samples: make([]prompb.Sample, samplesPerSeries),
		}
		// 填 sample(每 series 独立,避免 hash 撞车)
		for j := 0; j < samplesPerSeries; j++ {
			s.Samples[j] = prompb.Sample{
				Value:     rng.Float64() * 1000,
				Timestamp: time.Now().UnixMilli(),
			}
		}
		seriesPool = append(seriesPool, s)
	}

	// 编码 payload(预计算一次,所有 worker 复用)
	// 取前 seriesPerBatch 个 series 作为模板(每个 worker 选不同起点避免 hash 撞车)
	// 报告 payload 大小(取一次)
	{
		req := &prompb.WriteRequest{
			Timeseries: make([]prompb.TimeSeries, seriesPerBatch),
		}
		nowMs := time.Now().UnixMilli()
		for i := 0; i < seriesPerBatch; i++ {
			req.Timeseries[i].Labels = seriesPool[i].Labels
			req.Timeseries[i].Samples = make([]prompb.Sample, samplesPerSeries)
			for j := 0; j < samplesPerSeries; j++ {
				req.Timeseries[i].Samples[j] = prompb.Sample{
					Value:     rng.Float64() * 1000,
					Timestamp: nowMs,
				}
			}
		}
		raw, err := req.Marshal()
		if err != nil {
			log.Fatalf("marshal: %v", err)
		}
		encoded := snappy.Encode(nil, raw)
		fmt.Printf("payload_bytes=%d (snappy compressed)\n", len(encoded))
	}

	// 启动 worker
	var (
		wg            sync.WaitGroup
		stopCh        = make(chan struct{})
		latencies     sync.Mutex
		allLatencies  = make([]time.Duration, 0, totalBatches)
		sentBatches   atomic.Int64
		errBatches    atomic.Int64
		bytesSent     atomic.Int64
		status200     atomic.Int64
		statusOther   atomic.Int64
	)
	_ = status200
	_ = statusOther

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			MaxConnsPerHost:     200,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(batchInterval)
			defer ticker.Stop()
			// 每个 worker 自己的 rng,避免锁
			workerRng := rand.New(rand.NewSource(int64(workerID) + 1000))
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					start := time.Now()
					// 构造 payload
					req := &prompb.WriteRequest{
						Timeseries: make([]prompb.TimeSeries, seriesPerBatch),
					}
					nowMs := time.Now().UnixMilli()
					for i := 0; i < seriesPerBatch; i++ {
						// 从 seriesPool 选(轮转,避免 hash 撞车)
						idx := (workerID*7 + i) % len(seriesPool)
						ts := seriesPool[idx]
						req.Timeseries[i].Labels = ts.Labels
						req.Timeseries[i].Samples = make([]prompb.Sample, samplesPerSeries)
						for j := 0; j < samplesPerSeries; j++ {
							req.Timeseries[i].Samples[j] = prompb.Sample{
								Value:     workerRng.Float64() * 1000,
								Timestamp: nowMs,
							}
						}
					}
					raw, mErr := req.Marshal()
					if mErr != nil {
						errBatches.Add(1)
						continue
					}
					encoded := snappy.Encode(nil, raw)

					httpReq, _ := http.NewRequest(http.MethodPost, *url, bytes.NewReader(encoded))
					httpReq.Header.Set("Content-Type", "application/x-protobuf")
					httpReq.Header.Set("Content-Encoding", "snappy")
					httpReq.Header.Set("Authorization", "Bearer "+*token)
					resp, err := httpClient.Do(httpReq)
					if err != nil {
						errBatches.Add(1)
						if *verbose {
							fmt.Fprintf(os.Stderr, "worker %d: %v\n", workerID, err)
						}
						continue
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode < 200 || resp.StatusCode >= 300 {
						errBatches.Add(1)
					} else {
						bytesSent.Add(int64(len(encoded)))
					}
					sentBatches.Add(1)
					elapsed := time.Since(start)
					latencies.Lock()
					allLatencies = append(allLatencies, elapsed)
					latencies.Unlock()
				}
			}
		}(w)
	}

	// 统计 + 进度打印
	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()
	deadline := time.Now().Add(*duration)
	for time.Now().Before(deadline) {
		<-progressTicker.C
		elapsed := time.Since(deadline.Add(-*duration))
		sent := sentBatches.Load()
		errs := errBatches.Load()
		rateNow := float64(sent) * float64(*batchSize) / elapsed.Seconds()
		errRate := 0.0
		if sent > 0 {
			errRate = float64(errs) / float64(sent) * 100
		}
		fmt.Printf("t=%-4s rate=%-7.0f samples/s sent=%d err=%d (%.2f%%) bytes=%d\n",
			elapsed.Truncate(time.Second), rateNow, sent, errs, errRate, bytesSent.Load())
	}
	close(stopCh)
	wg.Wait()

	// 汇总
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	p50 := percentile(allLatencies, 0.50)
	p95 := percentile(allLatencies, 0.95)
	p99 := percentile(allLatencies, 0.99)
	maxLat := time.Duration(0)
	if len(allLatencies) > 0 {
		maxLat = allLatencies[len(allLatencies)-1]
	}
	totalSent := sentBatches.Load()
	totalErr := errBatches.Load()
	overallRate := float64(totalSent) * float64(*batchSize) / duration.Seconds()
	errRate := 0.0
	if totalSent > 0 {
		errRate = float64(totalErr) / float64(totalSent) * 100
	}

	fmt.Println()
	fmt.Println("=== Final ===")
	fmt.Printf("duration=%s sent_batches=%d err_batches=%d (%.4f%%) bytes=%d\n",
		*duration, totalSent, totalErr, errRate, bytesSent.Load())
	fmt.Printf("rate=%.0f samples/s\n", overallRate)
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", p50, p95, p99, maxLat)
	fmt.Printf("samples_sent=%d\n", totalSent*int64(*batchSize))

	// 拉取 GW 侧指标(可选)
	if *metricsURL != "" {
		fmt.Println()
		fmt.Println("=== GW metrics (selected) ===")
		resp, err := httpClient.Get(*metricsURL)
		if err != nil {
			fmt.Printf("metrics fetch error: %v\n", err)
		} else {
			defer resp.Body.Close()
			buf, _ := io.ReadAll(resp.Body)
			lines := bytes.Split(buf, []byte("\n"))
			for _, l := range lines {
				s := string(l)
				if bytes.HasPrefix(l, []byte("gateway_samples_total")) ||
					bytes.HasPrefix(l, []byte("gateway_errors_total")) ||
					bytes.HasPrefix(l, []byte("gateway_request_duration_seconds")) ||
					bytes.HasPrefix(l, []byte("gateway_backpressure_rejected_total")) ||
					bytes.HasPrefix(l, []byte("gateway_wal_bytes")) {
					fmt.Println("  " + s)
				}
			}
		}
	}
}

func percentile(s []time.Duration, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	idx := int(float64(len(s)) * p)
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
