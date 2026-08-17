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

/**
 * @class Batcher
 * @brief Aggregates independent requests into contiguous execution blocks.
 *
 * Optimizes system throughput by calculating the optimal batch size based on
 * available task duration tables and current queue saturation levels.
 */
class Batcher {
public:
    /**
     * @brief Extracts an optimal collection of requests from a processing queue.
     * @param q The target queue containing waiting request identifiers.
     * @param task_time_table The matrix of predefined hardware execution durations.
     * @return A vector of request identifiers forming the optimized batch.
     */
    static std::vector<int> pullBatch(std::queue<int>& q, const std::map<int, TaskDurations>& task_time_table) {
        int optimal_max = 16;
        
        std::vector<int> batch;
        while (!q.empty() && batch.size() < static_cast<size_t>(optimal_max)) {
            batch.push_back(q.front());
            q.pop();
        }
        return batch;
    }

    /**
     * @brief Formats a batch array into a standardized transmission string.
     * @param batch The collection of identifiers to format.
     * @return The string representation formatted for interactor ingestion.
     */
    static std::string formatBatchStr(const std::vector<int>& batch) {
        std::string res = std::to_string(batch.size());
        for (int id : batch) {
            res += " " + std::to_string(id);
        }
        return res;
    }
};
