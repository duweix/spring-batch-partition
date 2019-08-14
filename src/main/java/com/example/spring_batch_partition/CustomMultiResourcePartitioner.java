package com.example.spring_batch_partition;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

import lombok.Getter;
import lombok.Setter;

public class CustomMultiResourcePartitioner implements Partitioner {

    @Getter
    @Setter
    private Resource[] resources;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<>();
        int i = 0, k = 1;
        for (Resource resource : resources) {
            ExecutionContext context = new ExecutionContext();
            context.putString(keyName)
        }
        return null;
    }

}
