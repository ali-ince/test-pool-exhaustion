package com.neo4j.opsmanager.testpoolexhaustion;

import org.junit.jupiter.api.*;
import org.neo4j.driver.ConnectionPoolMetrics;
import org.neo4j.driver.Driver;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.neo4j.DataNeo4jTest;
import org.springframework.data.neo4j.core.ReactiveNeo4jTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@DataNeo4jTest
public class TestConnectionLeakage {
    private static Neo4j embeddedServer;

    @BeforeAll
    static void initializeNeo4j() {
        embeddedServer =
                Neo4jBuilders.newInProcessBuilder().withDisabledServer().build();
    }

    @AfterAll
    static void stopNeo4j() {
        embeddedServer.close();
    }

    private int inUseConnectionsBefore;

    /**
     * Generate 6 Person nodes of ids 0, 2, 4, 6, 8, 10
     *
     * @param neo4j
     */
    @BeforeEach
    void createData(@Autowired Driver neo4j) {
        try (var session = neo4j.session()) {
            session.run(
                            "UNWIND $list as id CREATE (:Person {id: id})",
                            Map.of(
                                    "list",
                                    IntStream.of(0, 2, 4, 6, 8, 10)
                                            .mapToObj(i -> String.valueOf(i))
                                            .collect(Collectors.toList())))
                    .consume();
        }
    }

    /**
     * Clean-up
     *
     * @param neo4j
     */
    @AfterEach
    void cleanup(@Autowired Driver neo4j) {
        try (var session = neo4j.session()) {
            session.run("MATCH (n:Person) DELETE n").consume();
        }
    }

    /**
     * Capture total number of in use connections from neo4j driver metrics before each
     * test.
     *
     * @param driver
     */
    @BeforeEach
    void captureInUseConnections(@Autowired Driver driver) {
        inUseConnectionsBefore = driver.metrics().connectionPoolMetrics().stream()
                .mapToInt(ConnectionPoolMetrics::inUse)
                .sum();
    }

    /**
     * Capture total number of in use connections from neo4j driver metrics after each test
     * and assert that it is equal to the one captured before test.
     *
     * @param driver
     */
    @AfterEach
    void assertInUseConnections(@Autowired Driver driver) {
        var inUseConnectionsAfter = driver.metrics().connectionPoolMetrics().stream()
                .mapToInt(ConnectionPoolMetrics::inUse)
                .sum();

        assertThat(inUseConnectionsAfter).isEqualTo(inUseConnectionsBefore);
    }

    @DynamicPropertySource
    static void neo4jProperties(DynamicPropertyRegistry registry) {

        registry.add("spring.neo4j.uri", embeddedServer::boltURI);
        registry.add("spring.neo4j.authentication.username", () -> "neo4j");
        registry.add("spring.neo4j.authentication.password", () -> null);
        registry.add("spring.neo4j.pool.metrics-enabled", () -> true);
    }

    /**
     * Reads all Person nodes in a flat map operation.
     * <p>
     * Should pass.
     *
     * @param neo4j
     */
    @Test
    public void readAll(@Autowired ReactiveNeo4jTemplate neo4j) {
        var people = Flux.fromIterable(List.of(0, 2, 4, 6, 8, 10))
                .flatMap(id -> neo4j.findById(String.valueOf(id), Person.class));

        StepVerifier.create(people).expectNextCount(6).verifyComplete();
    }

    /**
     * Try to read a series of Person nodes, most of which do not exist and the sub-stream is created
     * in a way to return an error in the case of non-existent Person node. Concurrency is 1, so each
     * sub-stream is executed serially.
     * <p>
     * Should pass.
     *
     * @param neo4j
     */
    @Test
    public void readWithSubStreamErrorConcurrencyOne(@Autowired ReactiveNeo4jTemplate neo4j) {
        var people = Flux.fromIterable(List.of(10, 9, 7, 5, 3, 1))
                .flatMap(
                        id -> neo4j.findById(String.valueOf(id), Person.class)
                                .switchIfEmpty(Mono.defer(() -> Mono.error(
                                        new RuntimeException(String.format("Person[id=%s] does not exist", id))))),
                        1);

        StepVerifier.create(people).consumeNextWith(p -> {}).expectError().verify();
    }

    /**
     * Try to read a series of Person nodes, most of which do not exist and the sub-stream is created
     * in a way to return an error in the case of non-existent Person node. Concurrency is default, so
     * sub-streams are executed in parallel resulting in dropped error messages.
     * <p>
     * Connections leak, so the in use connection assertion should fail.
     *
     * @param neo4j
     */
    @Test
    public void readWithSubStreamErrorConcurrencyDefault(@Autowired ReactiveNeo4jTemplate neo4j) {
        var people = Flux.fromIterable(List.of(10, 9, 7, 5, 3, 1))
                .flatMap(id -> neo4j.findById(String.valueOf(id), Person.class)
                        .switchIfEmpty(Mono.defer(() ->
                                Mono.error(new RuntimeException(String.format("Person[id=%s] does not exist", id))))));

        StepVerifier.create(people).consumeNextWith(p -> {}).expectError().verify();
    }
}
