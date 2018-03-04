package com.px.json;

import org.apache.avro.reflect.Nullable;
import org.apache.storm.validation.ConfigValidationAnnotations;

public class KafkaConfig {

    @ConfigValidationAnnotations.NotNull
    public String zkHosts;
    public String topicName;
    public String zkRoot;
    public String schemeType;

    @Override
    public String toString() {
        return "KafkaConfig{" +
                "zkHosts='" + zkHosts + '\'' +
                ", topicName='" + topicName + '\'' +
                ", zkRoot='" + zkRoot + '\'' +
                ", schemeType='" + schemeType + '\'' +
                '}';
    }
}
