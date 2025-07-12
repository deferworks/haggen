package co.deferworks.haggen.driver;

import co.deferworks.haggen.core.Job;

public interface Queue {
    Job enqueue(Job job);
}