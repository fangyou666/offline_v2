package com.v3;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ScokeStreamTest {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String>dataStreamSource=env.socketTextStream("cdh01",10265);
        dataStreamSource.print();

        env.execute();
    }
}
