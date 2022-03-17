package com.neo4j.opsmanager.testpoolexhaustion;

import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;

import java.util.Objects;

@Node("Person")
public class Person {
    @Id
    private final String id;

    public Person(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public String getId() {
        return this.id;
    }
}
