package com.saga.e2e;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A service jar running as a child JVM. Configuration is passed as Spring command-line arguments so it
 * overrides application.yml; stdout/stderr go to a per-service log file under target/e2e-logs.
 */
final class ServiceProcess implements AutoCloseable {

    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(120);

    private final String name;
    private final int port;
    private final Path logFile;
    private final Process process;

    private ServiceProcess(String name, int port, Path logFile, Process process) {
        this.name = name;
        this.port = port;
        this.logFile = logFile;
        this.process = process;
    }

    static ServiceProcess start(String name, Path jar, Path logDir, Map<String, String> properties) throws IOException {
        if (!Files.isRegularFile(jar)) {
            throw new IllegalStateException(jar + " not found - run 'mvn verify' from the root so services are packaged first");
        }
        int port = freePort();
        Files.createDirectories(logDir);
        Path logFile = logDir.resolve(name + ".log");

        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-jar");
        command.add(jar.toString());
        command.add("--server.port=" + port);
        properties.forEach((key, value) -> command.add("--" + key + "=" + value));

        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(logFile.toFile())
                .start();
        return new ServiceProcess(name, port, logFile, process);
    }

    void awaitHealthy(HttpClient http) throws InterruptedException {
        URI health = URI.create(baseUrl() + "/actuator/health");
        Instant deadline = Instant.now().plus(STARTUP_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) {
                throw new IllegalStateException(name + " exited with " + process.exitValue() + ", see " + logFile);
            }
            try {
                HttpResponse<String> response = http.send(
                        HttpRequest.newBuilder(health).timeout(Duration.ofSeconds(2)).build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200 && response.body().contains("\"UP\"")) {
                    return;
                }
            } catch (IOException notListeningYet) {
                // keep polling
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException(name + " not healthy within " + STARTUP_TIMEOUT + ", see " + logFile);
    }

    String baseUrl() {
        return "http://localhost:" + port;
    }

    @Override
    public void close() throws InterruptedException {
        process.destroy();
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
