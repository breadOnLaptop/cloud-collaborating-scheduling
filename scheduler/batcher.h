#pragma once

#include <vector>
#include <queue>
#include <string>

class Batcher {
public:
    static std::vector<int> pullBatch(std::queue<int>& q, int max_batch_size = 16) {
        std::vector<int> batch;
        while (!q.empty() && batch.size() < max_batch_size) {
            batch.push_back(q.front());
            q.pop();
        }
        return batch;
    }

    static std::string formatBatchStr(const std::vector<int>& batch) {
        std::string res = std::to_string(batch.size());
        for (int id : batch) {
            res += " " + std::to_string(id);
        }
        return res;
    }
};
