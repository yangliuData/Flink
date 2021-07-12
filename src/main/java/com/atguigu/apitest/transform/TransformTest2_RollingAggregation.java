package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author LiuYang
 * @Date 2021/7/6 3:31 下午
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/liuyang/IdeaProjects/FlinkTutorial/src/main/resources/sensor.txt");
        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] split = s.split(",");
//                return new SensorReading(split[0], new Long(split[2]), new Double(split[2]));
//            }
//        });
        // lamda 表达式
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
              return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
        KeyedStream<SensorReading, String> keyedStream2 = dataStream.keyBy(SensorReading::getId);
        // 滚动聚合，去当前最大温度值 及id 时间错
        /**
         * 这里要说一下max和maxBy的区别
         * max 温度的整体最大值 时间错不变
         * maxBy 都是当前最大值对应的那个时间错
         *
         */
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print("result");

       // keyedStream1.print("key1");
       // keyedStream2.sum(0).print("key2");


        env.execute();
    }
}
