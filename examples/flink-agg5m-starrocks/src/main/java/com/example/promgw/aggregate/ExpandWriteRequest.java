package com.example.promgw.aggregate;

import com.example.promgw.decoder.PromSample;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExpandWriteRequest 把一条 PromSample(含 N 个 series)展开为多条 SampleWithMeta。
 *
 * 输入:PromSample(timeseries=[series1, series2, ...], tenant, sourceDc, ...)
 * 输出:每 series 每 sample 一条 SampleWithMeta
 *
 * 例:WriteRequest 含 2 个 series,各 1 个 sample → 输出 2 条 SampleWithMeta
 */
public class ExpandWriteRequest implements FlatMapFunction<PromSample, SampleWithMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(ExpandWriteRequest.class);

    @Override
    public void flatMap(PromSample ps, Collector<SampleWithMeta> out) {
        if (ps == null || ps.getTimeseries() == null) {
            return;
        }

        for (PromSample.ParsedSeries series : ps.getTimeseries()) {
            if (series.getSamples() == null || series.getSamples().isEmpty()) {
                continue;
            }
            // 一个 series 通常 1 个 sample,但可能多个,逐个输出
            for (PromSample.ParsedSample s : series.getSamples()) {
                SampleWithMeta swm = new SampleWithMeta(
                        ps.getTenant(),
                        series.getMetricName(),
                        series.getLabels(),
                        s.getValue(),
                        s.getTimestampMs(),
                        ps.getSourceDc(),
                        ps.getIngestCity(),
                        ps.getIngestDc(),
                        ps.getIngestTimeMs(),
                        ps.getTraceparent()
                );
                out.collect(swm);
            }
        }
    }
}
