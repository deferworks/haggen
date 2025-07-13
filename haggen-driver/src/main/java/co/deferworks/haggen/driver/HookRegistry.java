package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class HookRegistry {

    private final Map<String, Consumer<Job>> onEnqueueHooks = new ConcurrentHashMap<>();
    private final Map<String, Consumer<Job>> onDequeueHooks = new ConcurrentHashMap<>();
    private final Map<String, Consumer<Job>> onCompleteHooks = new ConcurrentHashMap<>();
    private final Map<String, Consumer<Job>> onFailHooks = new ConcurrentHashMap<>();
    private final Map<String, Consumer<Job>> onDiscardHooks = new ConcurrentHashMap<>();
    private final Map<String, Consumer<Job>> onReapHooks = new ConcurrentHashMap<>();

    public void registerOnEnqueue(String jobKind, Consumer<Job> hook) {
        onEnqueueHooks.put(jobKind, hook);
    }

    public void registerOnDequeue(String jobKind, Consumer<Job> hook) {
        onDequeueHooks.put(jobKind, hook);
    }

    public void registerOnComplete(String jobKind, Consumer<Job> hook) {
        onCompleteHooks.put(jobKind, hook);
    }

    public void registerOnFail(String jobKind, Consumer<Job> hook) {
        onFailHooks.put(jobKind, hook);
    }

    public void registerOnDiscard(String jobKind, Consumer<Job> hook) {
        onDiscardHooks.put(jobKind, hook);
    }

    public void registerOnReap(String jobKind, Consumer<Job> hook) {
        onReapHooks.put(jobKind, hook);
    }

    public void executeOnEnqueue(Job job) {
        onEnqueueHooks.getOrDefault(job.kind(), j -> {}).accept(job);
    }

    public void executeOnDequeue(Job job) {
        onDequeueHooks.getOrDefault(job.kind(), j -> {}).accept(job);
    }

    public void executeOnComplete(Job job) {
        onCompleteHooks.getOrDefault(job.kind(), j -> {}).accept(job);
    }

    public void executeOnFail(Job job) {
        onFailHooks.getOrDefault(job.kind(), j -> {}).accept(job);
    }

    public void executeOnDiscard(Job job) {
        onDiscardHooks.getOrDefault(job.kind(), j -> {}).accept(job);
    }

    public void executeOnReap(Job job) {
        onReapHooks.getOrDefault(job.kind(), j -> {}).accept(job);
    }
}