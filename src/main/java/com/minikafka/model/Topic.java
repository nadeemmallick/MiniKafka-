package com.minikafka.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a topic in the MiniKafka system.
 * A topic contains multiple partitions.
 */
public class Topic {
    private String name;
    private List<Partition> partitions;

    public Topic(String name, int numPartitions) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic name cannot be null or empty");
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive");
        }
        
        this.name = name;
        this.partitions = new ArrayList<>();
        
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition(i));
        }
    }

    public String getName() {
        return name;
    }

    /**
     * Gets a partition by its ID.
     */
    public Partition getPartition(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }
        return partitions.get(partitionId);
    }

    /**
     * Gets all partitions in this topic.
     */
    public List<Partition> getPartitions() {
        return new ArrayList<>(partitions);
    }

    /**
     * Gets the number of partitions in this topic.
     */
    public int getNumPartitions() {
        return partitions.size();
    }
}
