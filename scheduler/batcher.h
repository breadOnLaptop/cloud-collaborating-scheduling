/**
 * @file batcher.h
 * @brief Batch consolidation logic for multi-request scheduling.
 *        Provides dual batch sizing: SLO2-bounded for latency mode,
 *        and max-throughput for throughput mode.
 * @author Authored by: opt1mal
 */

#pragma once

#include <vector>
#include <queue>
#include <string>
#include <unordered_map>
#include <charconv>
#include "../io/event_parser.h"

// Appends an integer to a string using to_chars (no heap temporaries)
inline void appendIntToString(std::string &s, int v) {
  char buf[32];
  auto r = std::to_chars(buf, buf + sizeof(buf), v);
  s.append(buf, r.ptr);
}

/**
 * @class Batcher
 * @brief Extracts and formats request batches for scheduling assignment.
 */
class Batcher {
public:
    /**
     * @brief Computes the largest batch size whose full decode cycle fits within SLO2.
     *        Used in latency-focused mode where TPOT must be controlled.
     */
    static int computeOptimalMax(const std::unordered_map<int, TaskDurations>& task_time_table, double slo2_limit, double S) {
        if (task_time_table.empty()) return 16;
        int optimal_max = 0;
        for (const auto& pair : task_time_table) {
            const auto& td = pair.second;
            if (td.d_pre < 0 || td.d_proc < 0 || td.d_post < 0) continue;
            double total_decode = 3.0 * S + td.d_pre + td.d_proc + td.d_post;
            if (total_decode <= slo2_limit) {
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

    /**
     * @brief Returns the largest batch size listed in the task-time table.
     *        Used in throughput-focused mode where TPOT is irrelevant (w_c low).
     *        Larger batches amortize setup cost S across more tokens per edge operation.
     */
    static int computeMaxBatch(const std::unordered_map<int, TaskDurations>& task_time_table) {
        int max_batch = 1;
        for (const auto& pair : task_time_table) {
            if (pair.first > max_batch) {
                max_batch = pair.first;
            }
        }
        return max_batch;
    }

    /**
     * @brief Extracts up to optimal_max eligible requests into a reusable buffer.
     */
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

    /**
     * @brief Appends a formatted batch descriptor directly into an output buffer.
     */
    static void appendBatchStr(std::string& out, const std::vector<int>& batch) {
        appendIntToString(out, (int)batch.size());
        for (int id : batch) {
            out.push_back(' ');
            appendIntToString(out, id);
        }
    }
};
