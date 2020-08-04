package com.function;

import com.azure.messaging.eventhubs.*;
import java.util.concurrent.TimeUnit;
import java.time.*;
import java.time.temporal.*;
import java.util.Arrays;

public class Sender {
    private static final int NUM_ITERATIONS = 3600;     // Number of iterations (an interation is fired every second, i.e. 3600 iteractions will run for 1 hour)
    private static final int NUM_EVENTS = 300;          // Number of events per iteration (i.e. per second)
    private static final int NUM_EVENTS_BURST = 1000;   // Number of events when bursting
    private static final int BURST_EVERY = 60;
    private static final int BURST_COUNT = 10;
    private static final int MESSAGE_SIZE_BYTES = 350;
    public static void main(String[] args) {
        final String connectionString = "Endpoint=sb://beet-eventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ThinDU+zXsevuwKaZKCvvnkJQHL5NTMmr4LskBTU6Ek=";

        // Create a producer using the namespace connection string and event hub name
        EventHubProducerAsyncClient producer = new EventHubClientBuilder()
            .connectionString(connectionString, "events")
            .buildAsyncProducerClient();

        // Prepare a batch of events to send to the event hub
        for (int iterations = 0; iterations < NUM_ITERATIONS; iterations++) {
            LocalDateTime startTime = LocalDateTime.now(ZoneOffset.UTC);
            int[] iterationsArray = {iterations};
            producer.createBatch().flatMap(batch -> {
                int interation = iterationsArray[0]; 
                int events = (interation != 0 && interation % BURST_EVERY == 0) ? NUM_EVENTS_BURST : NUM_EVENTS;
                System.out.println("Iteration: " + interation + ". Events: " + events);
                for (int i = 0; i < events; i++) {
                    byte[] bytes = new byte[MESSAGE_SIZE_BYTES];
                    Arrays.fill( bytes, (byte) 1 );
                    batch.tryAdd(new EventData("Message " + String.valueOf(interation) + "-" + String.valueOf(i) + ": " + Arrays.toString(bytes)));
                }
                return producer.send(batch);
            }).subscribe(unused -> {
            },
                error -> System.err.println("Error occurred while sending batch:" + error),
                () -> System.out.println("Send complete."));

            try {
                LocalDateTime finishTime = LocalDateTime.now(ZoneOffset.UTC);
                long millisBetween = ChronoUnit.MILLIS.between(startTime, finishTime);
                if (millisBetween <= 1000) {
                    System.out.println("Sleeping: " + (1000-millisBetween) + "ms");
                    TimeUnit.MILLISECONDS.sleep(1000-millisBetween);
                }
            } catch (InterruptedException ignored) {
            }
        }

        // Close the producer
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException ignored) {
        } finally {
            System.out.println("Closing publisher.");
            producer.close();
        }
    }
}