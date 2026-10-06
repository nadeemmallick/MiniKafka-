package com.minikafka.storage;

import com.minikafka.model.Message;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles file-based persistence for MiniKafka messages.
 * Stores messages in partition log files with format: offset|value|timestamp
 */
public class LogStorage {
    private final String dataDirectory;

    public LogStorage(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    /**
     * Initializes the storage directory structure.
     * Creates the data directory if it doesn't exist.
     */
    public void initializeStorage() {
        try {
            Path dataPath = Paths.get(dataDirectory);
            if (!Files.exists(dataPath)) {
                Files.createDirectories(dataPath);
                System.out.println("Created data directory: " + dataDirectory);
            } else {
                System.out.println("Data directory already exists: " + dataDirectory);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage directory: " + dataDirectory, e);
        }
    }

    /**
     * Appends a message to the appropriate partition log file.
     * Format: offset|value|timestamp
     */
    public void appendMessage(String topicName, int partitionId, Message message) {
        Path logFilePath = getLogFilePath(topicName, partitionId);
        
        try {
            // Ensure topic directory exists
            Path topicDir = logFilePath.getParent();
            if (!Files.exists(topicDir)) {
                Files.createDirectories(topicDir);
            }
            
            // Append message to log file
            String logEntry = message.getOffset() + "|" + message.getValue() + "|" + message.getTimestamp();
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath.toFile(), true))) {
                writer.write(logEntry);
                writer.newLine();
            }
            
        } catch (IOException e) {
            System.err.println("Failed to append message to log file: " + logFilePath);
            throw new RuntimeException("Failed to persist message", e);
        }
    }

    /**
     * Loads all messages from a partition log file.
     * Returns empty list if file doesn't exist or is empty.
     */
    public List<Message> loadMessages(String topicName, int partitionId) {
        Path logFilePath = getLogFilePath(topicName, partitionId);
        List<Message> messages = new ArrayList<>();
        
        if (!Files.exists(logFilePath)) {
            return messages; // Return empty list if file doesn't exist
        }
        
        try (BufferedReader reader = Files.newBufferedReader(logFilePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    Message message = parseLogEntry(line);
                    messages.add(message);
                } catch (Exception e) {
                    System.err.println("Failed to parse log entry, skipping: " + line);
                    // Skip corrupt lines and continue
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to read log file: " + logFilePath);
            throw new RuntimeException("Failed to load messages from storage", e);
        }
        
        return messages;
    }

    /**
     * Parses a log entry line into a Message object.
     * Expected format: offset|value|timestamp
     */
    private Message parseLogEntry(String line) {
        String[] parts = line.split("\\|");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid log entry format: " + line);
        }
        
        long offset = Long.parseLong(parts[0]);
        String value = parts[1];
        long timestamp = Long.parseLong(parts[2]);
        
        return new Message(offset, value, timestamp);
    }

    /**
     * Gets the file path for a specific partition log file.
     * Structure: dataDirectory/topicName/partition-partitionId.log
     */
    public Path getLogFilePath(String topicName, int partitionId) {
        return Paths.get(dataDirectory, topicName, "partition-" + partitionId + ".log");
    }

    /**
     * Checks if a topic has persisted data.
     */
    public boolean topicHasData(String topicName) {
        Path topicDir = Paths.get(dataDirectory, topicName);
        return Files.exists(topicDir);
    }

    /**
     * Gets the number of partitions for a topic based on existing log files.
     * Returns 0 if topic doesn't exist.
     */
    public int getPartitionCount(String topicName) {
        Path topicDir = Paths.get(dataDirectory, topicName);
        if (!Files.exists(topicDir)) {
            return 0;
        }
        
        try {
            File[] files = topicDir.toFile().listFiles((dir, name) -> name.startsWith("partition-") && name.endsWith(".log"));
            return files != null ? files.length : 0;
        } catch (Exception e) {
            System.err.println("Failed to count partitions for topic: " + topicName);
            return 0;
        }
    }

    /**
     * Clears all persisted data (for testing purposes).
     */
    public void clearAllData() {
        try {
            Path dataPath = Paths.get(dataDirectory);
            if (Files.exists(dataPath)) {
                Files.walk(dataPath)
                    .sorted((a, b) -> b.compareTo(a)) // Delete files first, then directories
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + path);
                        }
                    });
                Files.deleteIfExists(dataPath);
                System.out.println("Cleared all data from: " + dataDirectory);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear storage data", e);
        }
    }
}