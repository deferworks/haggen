package co.deferworks.haggen.core;

@FunctionalInterface
public interface JobHandler {
    void execute(Job job) throws Exception;
}
