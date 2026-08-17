/**
 * @file batcher.h
 * @brief Batch consolidation logic for multi-request scheduling.
 * @author Authored by: opt1mal
 */

#pragma once

#include <vector>
#include <queue>
#include <string>
#include <unordered_map>
#include <charconv>
#include "../io/event_parser.h"

inline void appendIntToString(std::string &s, int v) {
  char buf[32];
  auto r = std::to_chars(buf, buf + sizeof(buf), v);
  s.append(buf, r.ptr);
}

class Batcher {
public:
    static int computeOptimalMax(const std::unordered_map<int, TaskDurations>& task_time_table, double slo2_limit) {
        if (task_time_table.empty()) return 16;
        int optimal_max = 0;
        for (const auto& pair : task_time_table) {
            if (pair.second.d_proc <= slo2_limit * 0.5) {
                if (pair.first > optimal_max) {
                    optimal_max = pair.first;
                }
            }
        }
        if (optimal_max == 0) {
            int min_batch = 1e9;
            for (const auto& pair : task_time_table) {
                if (pair.first < min_batch) min_batch = pair.first;
            }
            return min_batch;
        }
        return optimal_max;
    }

    // Writes into a pre-allocated vector to avoid heap allocation per call
    static void pullBatch(std::queue<int>& q, int optimal_max, const SystemState& state, std::vector<int>& batch) {
        batch.clear();
        while (!q.empty() && batch.size() < static_cast<size_t>(optimal_max)) {
            int id = q.front();
            q.pop();
            if (state.all_request[id].state == RequestState::FINISHED) {
                continue;
            }
            batch.push_back(id);
        }
    }

    // Appends formatted batch directly into an existing output buffer
    static void appendBatchStr(std::string& out, const std::vector<int>& batch) {
        appendIntToString(out, (int)batch.size());
        for (int id : batch) {
            out.push_back(' ');
            appendIntToString(out, id);
        }
    }
};
