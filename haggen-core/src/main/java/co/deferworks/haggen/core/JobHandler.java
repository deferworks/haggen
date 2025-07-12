package co.deferworks.haggen.core;

@FunctionalInterface
public interface JobHandler {
    void handle(Job job) throws Exception;
}
