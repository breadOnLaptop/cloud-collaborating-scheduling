/**
 * @file batcher.h
 * @brief Batch consolidation logic for multi-request scheduling.
 * @author Authored by: opt1mal
 */

#pragma once

#include <vector>
#include <queue>
#include <string>
#include <map>
#include "../io/event_parser.h"

class Batcher {
public:
    static std::vector<int> pullBatch(std::queue<int>& q, const std::map<int, TaskDurations>& task_time_table, double slo2_limit = 1e9) {
        int optimal_max = 16;
        if (!task_time_table.empty()) {
            optimal_max = 0;
            for (const auto& pair : task_time_table) {
                // Ensure we don't pick a batch size that takes so long it violates SLO2
                if (pair.second.d_proc <= slo2_limit * 0.5) {
                    if (pair.first > optimal_max) {
                        optimal_max = pair.first;
                    }
                }
            }
            // Fallback to the smallest batch size if all exceed the safe SLO2 threshold
            if (optimal_max == 0) {
                optimal_max = task_time_table.begin()->first;
            }
        }
        
        std::vector<int> batch;
        while (!q.empty() && batch.size() < static_cast<size_t>(optimal_max)) {
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
