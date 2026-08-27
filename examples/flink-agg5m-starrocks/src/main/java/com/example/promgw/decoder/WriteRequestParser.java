package com.example.promgw.decoder;

import com.example.promgw.proto.PromProtos;
import com.example.promgw.proto.PromProtos.WriteRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * WriteRequestParser 解析 prompb.WriteRequest protobuf 字节 → POJO。
 *
 * schema 见 src/main/proto/remote.proto + types.proto
 * (与 prom-gw/api/proto 对齐,去 gogoproto 依赖)
 */
public final class WriteRequestParser {

    private WriteRequestParser() {}

    /** parse 把 protobuf 字节解析为 ParsedWriteRequest。 */
    public static PromSample parse(byte[] protobufBytes,
                                   Map<String, String> headers) throws IOException {
        WriteRequest req = WriteRequest.parseFrom(protobufBytes);

        List<PromSample.ParsedSeries> seriesList = new ArrayList<>(req.getTimeseriesCount());
        for (PromProtos.TimeSeries ts : req.getTimeseriesList()) {
            // 提取 metric name(__name__ label)和其余 labels
            String metricName = "";
            Map<String, String> labels = new HashMap<>(ts.getLabelsCount());
            for (PromProtos.Label lp : ts.getLabelsList()) {
                if ("__name__".equals(lp.getName())) {
                    metricName = lp.getValue();
                } else {
                    labels.put(lp.getName(), lp.getValue());
                }
            }

            // 解析 samples(通常 1 个,可能有多个)
            List<PromSample.ParsedSample> samples = new ArrayList<>(ts.getSamplesCount());
            for (PromProtos.Sample s : ts.getSamplesList()) {
                samples.add(new PromSample.ParsedSample(
                        s.getValue(),
                        s.getTimestamp()   // 毫秒,与 prom-gw 对齐
                ));
            }

            seriesList.add(new PromSample.ParsedSeries(metricName, labels, samples));
        }

        // 从 header 提取业务/机房信息(payload 里没有这些)
        String business     = headers != null ? getOrDefault(headers, "business", "") : "";
        String sourceDc     = headers != null ? getOrDefault(headers, "source_dc", "") : "";
        String ingestCity   = headers != null ? getOrDefault(headers, "ingest_city", "") : "";
        String ingestDc     = headers != null ? getOrDefault(headers, "ingest_dc", "") : "";
        String ingestTimeMs = headers != null ? getOrDefault(headers, "ingest_time_ms", "0") : "0";
        String traceparent  = headers != null ? getOrDefault(headers, "traceparent", "") : "";

        return new PromSample(
                seriesList, business, sourceDc, ingestCity, ingestDc,
                parseLongSafe(ingestTimeMs), traceparent
        );
    }

    private static String getOrDefault(Map<String, String> map, String key, String def) {
        String v = map.get(key);
        return v == null ? def : v;
    }

    private static long parseLongSafe(String s) {
        if (s == null || s.isEmpty()) return 0L;
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return 0L; }
    }
}