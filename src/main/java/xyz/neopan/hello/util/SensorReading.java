package xyz.neopan.hello.util;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.val;

import java.io.Serializable;

@Value(staticConstructor = "of")
@EqualsAndHashCode(of = "id")
public class SensorReading implements Serializable {

    /**
     * sensor id
     */
    String id;
    /**
     * event time
     */
    long timestamp;
    /**
     * sensor data: temperature
     */
    double temperature;

    /**
     * F -> C
     *
     * @return celsius temperature
     */
    public SensorReading toCTemp() {
        val celsius = (temperature - 32) / (5.0 / 9.0);
        return SensorReading.of(id, timestamp, celsius);
    }

}
