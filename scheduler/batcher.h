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

#include <charconv>

inline void appendIntToString(std::string &s, int v) {
  char buf[32];
  auto r = std::to_chars(buf, buf + sizeof(buf), v);
  s.append(buf, r.ptr);
}

class Batcher {
public:
    static int computeOptimalMax(const std::map<int, TaskDurations>& task_time_table, double slo2_limit) {
        if (task_time_table.empty()) return 16;
        int optimal_max = 0;
        for (const auto& pair : task_time_table) {
            if (pair.second.d_proc <= slo2_limit * 0.5) {
                if (pair.first > optimal_max) {
                    optimal_max = pair.first;
                }
            }
        }
        return optimal_max == 0 ? task_time_table.begin()->first : optimal_max;
    }

    static std::vector<int> pullBatch(std::queue<int>& q, int optimal_max, const SystemState& state) {
        std::vector<int> batch;
        while (!q.empty() && batch.size() < static_cast<size_t>(optimal_max)) {
            int id = q.front();
            q.pop();
            if (state.all_request[id].state == RequestState::FINISHED) {
                continue;
            }
            batch.push_back(id);
        }
        return batch;
    }

    static std::string formatBatchStr(const std::vector<int>& batch) {
        std::string res;
        res.reserve(batch.size() * 4 + 16);
        appendIntToString(res, (int)batch.size());
        for (int id : batch) {
            res.push_back(' ');
            appendIntToString(res, id);
        }
        return res;
    }
};
