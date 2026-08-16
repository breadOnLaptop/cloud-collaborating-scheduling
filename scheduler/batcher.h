#pragma once

#include <vector>
#include <queue>
#include <string>

class Batcher {
public:
    // Extracts up to 'max_batch_size' requests from a waiting queue.
    // Batching spreads the setup penalty (S) over multiple requests, drastically improving throughput.
    // In advanced versions, max_batch_size can be calculated dynamically by reading the Task-Time table.
    static std::vector<int> pullBatch(std::queue<int>& q, int max_batch_size = 16) {
        std::vector<int> batch;
        while (!q.empty() && batch.size() < max_batch_size) {
            batch.push_back(q.front());
            q.pop();
        }
        return batch;
    }

    // Helper to format the batch IDs into a space-separated string for the interactor
    // e.g., returns "3 14 2 9" (where 3 is the count 'm', followed by the request IDs)
    static std::string formatBatchStr(const std::vector<int>& batch) {
        std::string res = std::to_string(batch.size());
        for (int id : batch) {
            res += " " + std::to_string(id);
        }
        return res;
    }
};
