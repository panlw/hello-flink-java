package xyz.neopan.hello.util;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * @author neopan
 * @since 2020/5/8
 */
public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    public SensorTimeAssigner() {
        super(Time.of(5, TimeUnit.SECONDS));
    }

    @Override
    public long extractTimestamp(SensorReading element) {
        return element.getTimestamp();
    }

}
