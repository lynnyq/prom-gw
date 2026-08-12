package com.example.promgw.decoder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaRecordDeserializer 自定义 Kafka 反序列化器,保留 key + value + headers。
 *
 * Flink 默认的 valueOnlyDeserializer 会丢弃 key 和 headers,而 prom-gw 的租户/机房
 * 信息都在 headers 里,必须自定义。
 */
public class KafkaRecordDeserializer implements KafkaRecordDeserializationSchema<KafkaRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordDeserializer.class);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, org.apache.flink.util.Collector<KafkaRecord> out) {
        // 提取所有 Kafka headers(prom-gw 写入的 tenant/source_dc/ingest_city/...)
        Map<String, String> headers = new HashMap<>();
        Headers hdrs = record.headers();
        if (hdrs != null) {
            for (Header h : hdrs) {
                headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }
        }

        KafkaRecord kr = new KafkaRecord(
                record.key(),
                record.value(),
                record.topic(),
                record.offset(),
                record.partition(),
                record.timestamp(),
                headers
        );

        if (LOG.isDebugEnabled()) {
            LOG.debug("deserialized kafka record: {}", kr);
        }
        out.collect(kr);
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
