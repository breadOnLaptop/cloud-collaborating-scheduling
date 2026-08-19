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
    /**
     * @brief Finds the largest batch where d_proc ≤ SLO2 × 0.5.
     *        The 0.5 multiplier leaves headroom for S, d_pre, d_post, and transfers.
     *        Empirically gives larger (better-amortized) batches than the full-cycle formula.
     */
    static int computeOptimalMax(const std::unordered_map<int, TaskDurations>& task_time_table, double slo2_limit) {
        if (task_time_table.empty()) return 16;
        int optimal_max = 0;
        for (const auto& pair : task_time_table) {
            if (pair.second.d_proc < 0) continue;
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

    /**
     * @brief Full-cycle SLO2 bound: 3S + d_pre + d_proc + d_post + 2*transfer ≤ SLO2.
     *        Accounts for setup penalties, all processing stages, and network transfers.
     *        Gives tighter (smaller) batches that prevent TPOT violations, maximizing
     *        norm_c for latency-dominated tests (w_c >> w_tp).
     */
    static int computeStrictMax(const std::unordered_map<int, TaskDurations>& task_time_table,
                                double slo2_limit, const SystemParams& params) {
        if (task_time_table.empty()) return 1;
        
        // Estimate one-way transfer time per batch:
        // transfer_ms = latency + (bytes_per_token * batch_size * 8) / (bandwidth_gbps * 1e6)
        int strict_max = 0;
        for (const auto& pair : task_time_table) {
            const auto& td = pair.second;
            if (td.d_pre < 0 || td.d_proc < 0 || td.d_post < 0) continue;
            
            int batch = pair.first;
            double transfer_one_way = params.latency +
                (static_cast<double>(params.bytes_per_token) * batch * 8.0) / (params.bandwidth * 1e6);
            double total_cycle = 3.0 * params.S + td.d_pre + td.d_proc + td.d_post + 2.0 * transfer_one_way;
            
            if (total_cycle <= slo2_limit) {
                if (batch > strict_max) {
                    strict_max = batch;
                }
            }
        }
        if (strict_max == 0) {
            // Nothing fits — use smallest batch (minimum TPOT)
            int min_batch = 1e9;
            for (const auto& pair : task_time_table) {
                if (pair.first < min_batch) min_batch = pair.first;
            }
            return min_batch;
        }
        return strict_max;
    }

    /**
     * @brief Returns the largest batch size in the table.
     *        Used when TPOT is irrelevant (w_c ≈ 0).
     */
    static int computeMaxBatch(const std::unordered_map<int, TaskDurations>& task_time_table) {
        int max_batch = 1;
        for (const auto& pair : task_time_table) {
            if (pair.first > max_batch) max_batch = pair.first;
        }
        return max_batch;
    }

    static void pullBatch(std::queue<int>& q, int optimal_max, const SystemState& state, std::vector<int>& batch) {
        batch.clear();
        while (!q.empty() && batch.size() < static_cast<size_t>(optimal_max)) {
            int id = q.front();
            q.pop();
            if (state.all_request[id].state == RequestState::FINISHED) continue;
            batch.push_back(id);
        }
    }

    static void appendBatchStr(std::string& out, const std::vector<int>& batch) {
        appendIntToString(out, (int)batch.size());
        for (int id : batch) {
            out.push_back(' ');
            appendIntToString(out, id);
        }
    }
};
