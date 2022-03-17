package com.neo4j.opsmanager.testpoolexhaustion;

import org.springframework.data.neo4j.repository.ReactiveNeo4jRepository;

public interface PersonRepository extends ReactiveNeo4jRepository<Person, String> {
}
