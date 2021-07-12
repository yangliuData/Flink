package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Author LiuYang
 * @Date 2021/7/12 3:47 下午
 */
public class MyPeriodicassigner implements AssignerWithPeriodicWatermarks<SensorReading> {

    private  Long bound = 60 * 1000L; // 延迟一分钟
    private  Long maxTs = Long.MIN_VALUE; // 当前最大时间戳
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs - bound);
    }

    @Override
    public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
        maxTs = Math.max(maxTs,element.getTimestamp());
        return element.getTimestamp();
    }
}
