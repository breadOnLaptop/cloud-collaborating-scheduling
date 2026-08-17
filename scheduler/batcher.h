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
    static std::vector<int> pullBatch(std::queue<int>& q, const std::map<int, TaskDurations>& task_time_table, double slo2_limit, const SystemState& state) {
        // Cache the optimal max batch size statically so we compute it exactly once (O(1) after first call)
        static int cached_optimal_max = -1;
        
        if (cached_optimal_max == -1) {
            int optimal_max = 16; // Safe default
            if (!task_time_table.empty()) {
                optimal_max = 0;
                for (const auto& pair : task_time_table) {
                    // Pick the absolute maximum batch size that completes D_PROC well within the SLO2 limit
                    if (pair.second.d_proc <= slo2_limit * 0.8) {
                        if (pair.first > optimal_max) {
                            optimal_max = pair.first;
                        }
                    }
                }
                // If the hardware is so slow that even the smallest batch exceeds SLO2, 
                // fallback to the absolute smallest available batch size to minimize failure
                if (optimal_max == 0) {
                    optimal_max = task_time_table.begin()->first;
                }
            }
            cached_optimal_max = optimal_max;
        }
        
        std::vector<int> batch;
        while (!q.empty() && batch.size() < static_cast<size_t>(cached_optimal_max)) {
            int id = q.front();
            q.pop();
            // Lazily skip any requests that finished early
            if (state.all_request[id].state == RequestState::FINISHED) {
                continue;
            }
            batch.push_back(id);
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
