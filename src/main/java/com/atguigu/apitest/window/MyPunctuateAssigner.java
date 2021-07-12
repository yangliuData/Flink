package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Author LiuYang
 * @Date 2021/7/12 5:06 下午
 */
public class MyPunctuateAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {
    private Long bound = 60 * 1000L; // 延迟一分钟
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {
        if(lastElement.getId().equals("sensor_1"))  // 特定场景下 只有sensor_1 出现才会生成watermark
            return new Watermark(extractedTimestamp - bound);
        return null;
    }

    @Override
    public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
