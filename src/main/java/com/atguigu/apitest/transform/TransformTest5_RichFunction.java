package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author LiuYang
 * @Date 2021/7/8 9:51 上午
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> dataStreamSource = env.readTextFile("/Users/liuyang/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = dataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map( new MyMapper() );

        resultStream.print();

        env.execute();
    }
    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }
    // 实现自定义富函数类
    private static class MyMapper extends RichMapFunction<SensorReading,Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {

            return new Tuple2<>(value.getId(),getRuntimeContext().getAttemptNumber());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }
        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }
}
