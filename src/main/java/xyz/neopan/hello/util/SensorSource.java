package xyz.neopan.hello.util;

import lombok.val;
import lombok.var;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import scala.Tuple2;
import scala.util.Random;

import java.util.Calendar;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        val rand = new Random();
        val taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        var curFTemp = IntStream.range(1, 10)
                .mapToObj(i -> Tuple2.apply(
                        "sensor_" + (taskIdx * 10 + i),
                        65 + (rand.nextGaussian() * 20))
                ).collect(Collectors.toList());

        while (running) {
            // update data per loop
            curFTemp = curFTemp.stream()
                    .map(x -> Tuple2.apply(
                            x._1,
                            x._2 + (rand.nextGaussian() * 0.5)
                    )).collect(Collectors.toList());

            // collect data with current time (epoch)
            val curTime = Calendar.getInstance().getTimeInMillis();
            curFTemp.forEach(x -> ctx.collect(SensorReading.of(x._1, curTime, x._2)));

            //noinspection BusyWait
            Thread.sleep(100); // sleep 0.1s
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
