//second
package client;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MultithreadedClient {
    private static final int NUM_THREADS = 32;
    private static final int NUM_REQUESTS_PER_THREAD = 1000;
    private static final int TOTAL_REQUESTS =10000;
    private static final String SERVER_URL = "http://168.138.73.201:8080";
    private static final BlockingQueue<SkierLiftRideEventGenerator.SkierLiftRideEvent> eventQueue = new LinkedBlockingQueue<>();
    private static final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    private static final ExecutorService eventExecutor = Executors.newSingleThreadExecutor();

    private static AtomicInteger successfulRequests = new AtomicInteger(0);
    private static int unsuccessfulRequests = 0;
    private static final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private static final List<Integer> responseCodes = Collections.synchronizedList(new ArrayList<>());
    private static final CountDownLatch latch = new CountDownLatch(TOTAL_REQUESTS);
    
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis(); // Record the start time

        // Start event generation thread
        eventExecutor.execute(new EventGenerationTask());

        // Start initial threads for 1000 POST requests each
        for (int i = 0; i < NUM_THREADS; ++i) {
            executor.execute(new APIClientTask(NUM_REQUESTS_PER_THREAD));
        }

        try {
            // Wait until all requests are completed
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis(); // Record the end time

        // Print statistics
        System.out.println("Number of successful requests sent: " + successfulRequests);
        System.out.println("Number of unsuccessful requests: " + unsuccessfulRequests);
        System.out.println("Total run time: " + (endTime - startTime) + " milliseconds");

        double throughput = (double) (successfulRequests.get() + unsuccessfulRequests) / ((endTime - startTime) / 1000.0); // requests per second
        System.out.println("Total throughput: " + throughput + " requests per second");

        calculateStatistics();

        // Shut down the executor services
        executor.shutdown();
        eventExecutor.shutdown();
    }


    static class APIClientTask implements Runnable {
        private final int numRequests;
        private final int MAX_RETRIES = 5;
        private final long RETRY_DELAY_MS = 1000; // 1 second delay between retries

        public APIClientTask(int numRequests) {
            this.numRequests = numRequests;
        }

        public void run() {
            for (int i = 0; i < numRequests; ++i) {
                try {
                    performRequest();
                } catch (IOException | URISyntaxException | InterruptedException e) {
                    e.printStackTrace();
                    unsuccessfulRequests++;
                }
            }
        }

        private void performRequest() throws IOException, URISyntaxException, InterruptedException {
            long startTime = System.currentTimeMillis(); // Record start time
            SkierLiftRideEventGenerator.SkierLiftRideEvent event = eventQueue.take();
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(event);
            HttpClient client = HttpClient.newHttpClient();

            int retryCount = 0;
            HttpResponse<String> response = null;

            while (retryCount < MAX_RETRIES) {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(new URI(SERVER_URL + "/skiers"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(json))
                            .build();

                    response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    // Check if response code is in the range of 4XX or 5XX
                    if (response.statusCode() >= 400 && response.statusCode() < 600) {
                        retryCount++;
                        System.out.println("Thread " + Thread.currentThread().getId() +
                                " - Retry attempt " + retryCount +
                                " - Response code: " + response.statusCode() +
                                " - Retrying...");
                    } else {
                        break; // Exit loop if response code is not in 4XX or 5XX range
                    }
                } catch (ConnectException | ClosedChannelException e) {
                    // Connection failure or closed channel, retry after a delay
                    System.out.println("Connection failure or closed channel. Retrying after delay...");
                    Thread.sleep(RETRY_DELAY_MS);
                    retryCount++;
                }
            }

            long endTime = System.currentTimeMillis(); // Record end time
            long latency = endTime - startTime; // Calculate latency

            if (response != null && response.statusCode() >= 200 && response.statusCode() < 300) {
                successfulRequests.incrementAndGet(); // Increment successfulRequests atomically
            } else {
                unsuccessfulRequests++;
                System.out.println("Thread " + Thread.currentThread().getId() +
                        " - Request failed with status code: " + (response != null ? response.statusCode() : "N/A"));
            }

            latencies.add(latency); // Store latency
            if (response != null) {
                responseCodes.add(response.statusCode()); // Store response code
            }

            System.out.println("Thread: " + Thread.currentThread().getId() +
                    " - Response code: " + (response != null ? response.statusCode() : "N/A") +
                    " - Response body: " + (response != null ? response.body() : "N/A") +
                    " - Latency: " + latency + " milliseconds");

            // Count down the latch to signal completion of this request
            latch.countDown();

            if (retryCount == MAX_RETRIES) {
                System.out.println("Thread " + Thread.currentThread().getId() + " reached maximum retries, terminating.");
            }
        }
    }


    static class EventGenerationTask implements Runnable {
        private final SkierLiftRideEventGenerator generator = new SkierLiftRideEventGenerator();

        private volatile boolean stopRequested = false;

        EventGenerationTask() {
        }

        public void run() {
            try {
                for (int i = 0; i < TOTAL_REQUESTS && !stopRequested; i++) {
                    SkierLiftRideEventGenerator.SkierLiftRideEvent event = this.generator.generateEvent();
                    eventQueue.put(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void stop() {
            stopRequested = true;
        }
    }

    static void calculateStatistics() {
        // Calculate mean response time
        double meanResponseTime = latencies.stream().mapToLong(Long::valueOf).average().orElse(0.0);
        System.out.println("Mean response time: " + meanResponseTime + " milliseconds");

        // Calculate median response time
        Collections.sort(latencies);
        double medianResponseTime;
        if (latencies.size() % 2 == 0) {
            medianResponseTime = (latencies.get(latencies.size() / 2) + latencies.get(latencies.size() / 2 - 1)) / 2.0;
        } else {
            medianResponseTime = latencies.get(latencies.size() / 2);
        }
        System.out.println("Median response time: " + medianResponseTime + " milliseconds");

        // Calculate p99 response time
        if (!latencies.isEmpty()) {
            int p99Index = (int) Math.ceil(latencies.size() * 0.99) - 1; // Adjust index calculation
            double p99ResponseTime = latencies.get(p99Index);
            System.out.println("p99 response time: " + p99ResponseTime + " milliseconds");
        } else {
            System.out.println("Unable to calculate p99 response time. Latencies list is empty.");
        }

        // Calculate min and max response times
        long minResponseTime = latencies.isEmpty() ? 0 : Collections.min(latencies);
        long maxResponseTime = latencies.isEmpty() ? 0 : Collections.max(latencies);
        System.out.println("Minimum response time: " + minResponseTime + " milliseconds");
        System.out.println("Maximum response time: " + maxResponseTime + " milliseconds");

        // Write CSV file
        try (FileWriter writer = new FileWriter("response_times.csv")) {
            writer.write("Start Time, Request Type, Latency, Response Code\n");
            for (int i = 0; i < latencies.size(); i++) {
                long startTime = System.currentTimeMillis() - latencies.get(i); // Calculate start time
                long latency = latencies.get(i); // Get latency
                int responseCode = responseCodes.get(i); // Get response code
                writer.write(startTime + ", POST, " + latency + ", " + responseCode + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
