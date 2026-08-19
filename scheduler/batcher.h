/**
 * @file batcher.h
 * @brief Batch consolidation logic for multi-request scheduling.
 *        Provides SLO2-bounded batch sizing using full decode cycle estimation
 *        and zero-allocation formatting.
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
 *
 * Determines the maximum safe batch size from the task duration table using
 * full decode cycle estimation (d_pre + d_proc + d_post + 3×S ≤ SLO2),
 * pulls eligible requests from queues, and formats them for output.
 */
class Batcher {
public:
    /**
     * @brief Computes the largest batch size whose full decode cycle fits within SLO2.
     *        Uses d_pre + d_proc + d_post + 3×S as the decode cycle estimate,
     *        which is more accurate than checking d_proc alone.
     * @param task_time_table Mapping of batch sizes to their execution durations.
     * @param slo2_limit The maximum allowable token latency (TPOT target).
     * @param S The setup penalty time added to every task.
     * @return The optimal maximum batch size.
     */
    static int computeOptimalMax(const std::unordered_map<int, TaskDurations>& task_time_table, double slo2_limit, double S) {
        if (task_time_table.empty()) return 16;
        int optimal_max = 0;
        for (const auto& pair : task_time_table) {
            const auto& td = pair.second;
            // Skip entries with missing decode durations (marked as -1)
            if (td.d_pre < 0 || td.d_proc < 0 || td.d_post < 0) continue;

            // Full decode cycle: 3 setup penalties + all three decode step durations
            double total_decode = 3.0 * S + td.d_pre + td.d_proc + td.d_post;
            if (total_decode <= slo2_limit) {
                if (pair.first > optimal_max) {
                    optimal_max = pair.first;
                }
            }
        }
        if (optimal_max == 0) {
            // Fallback: use smallest available batch size
            int min_batch = 1e9;
            for (const auto& pair : task_time_table) {
                if (pair.first < min_batch) min_batch = pair.first;
            }
            return min_batch;
        }
        return optimal_max;
    }

    /**
     * @brief Extracts up to optimal_max eligible requests into a reusable buffer.
     * @param q Source queue of request IDs.
     * @param optimal_max Maximum batch capacity.
     * @param state System state for lifecycle checks.
     * @param batch Pre-allocated output buffer (cleared on entry).
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
     * @param out Target string buffer.
     * @param batch The batch of request IDs to format.
     */
    static void appendBatchStr(std::string& out, const std::vector<int>& batch) {
        appendIntToString(out, (int)batch.size());
        for (int id : batch) {
            out.push_back(' ');
            appendIntToString(out, id);
        }
    }
};
