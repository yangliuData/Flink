package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author LiuYang
 * @Date 2021/7/8 10:26 上午
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> inputStream = env.readTextFile("/Users/liuyang/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt");
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("input");

        // 1. shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();

        shuffleStream.print("shuffle");

        // 2. keyBy

        dataStream.keyBy("id").print("keyBy");

        // 3. global
        dataStream.global().print("global");

        env.execute();
    }
}
