/**
 * @file advanced_scheduler.h
 * @brief Primary decision engine for intelligent task distribution.
 *        Implements a fully dynamic, self-adjusting architecture that analyzes
 *        the input test requirements (w_tp, w_c, SLOs) and selects the globally
 *        optimal scheduling strategy (Extreme Throughput, Balanced, or Strict Latency).
 * @author Authored by: opt1mal
 */

#pragma once

#include <vector>
#include <string>
#include <queue>
#include <set>
#include <utility>
#include "../state/system_state.h"
#include "../io/event_parser.h"
#include "chunker.h"
#include "batcher.h"

enum class StrategyMode {
    LATENCY_STRICT,
    BALANCED,
    THROUGHPUT_EXTREME
};

/**
 * @class AdvancedScheduler
 * @brief Orchestrates task assignment dynamically based on exact test case requirements.
 *
 * It adjusts 3 dimensions dynamically based on (w_tp, w_c):
 * 1. THROUGHPUT_EXTREME (w_c < 0.01) -> e.g. Test 19
 *    - Batch: Maximum possible.
 *    - Edge Priority: D_POST > P_POST > P_PRE > D_PRE. By deferring D_PRE, it allows
 *      prefills to complete entirely, accumulating massive batches of 100+ requests
 *      for decode, drastically reducing edge operations.
 *    - Cloud: Bursts all prefill layers at once.
 *
 * 2. BALANCED (w_tp > w_c >= 0.01) -> e.g. Tests 6, 13, 14
 *    - Batch: Maximum possible for better throughput amortization.
 *    - Edge Priority: D_POST > P_POST > P_PRE (cloud idle) > D_PRE > P_PRE.
 *    - Cloud: Chunks prefill to 8 layers to yield to decode pressure.
 *
 * 3. LATENCY_STRICT (w_c >= w_tp) -> e.g. Tests 4, 10, 22
 *    - Batch: Bounded strictly (d_proc <= SLO2 * 0.5) to guarantee TPOT.
 *    - Edge Priority: D_POST > P_POST > P_PRE (cloud idle) > D_PRE > P_PRE.
 *    - Cloud: Chunks prefill to 8 layers to yield to decode pressure.
 */
class AdvancedScheduler {
private:
    std::vector<double> total_assigned_work;
    std::set<std::pair<double, int>> cloud_load;
    int cached_optimal_max = -1;
    StrategyMode current_strategy = StrategyMode::LATENCY_STRICT;
    std::vector<int> batch_buf;

    void cleanQueue(std::queue<int>& q, const SystemState& state) {
        while (!q.empty() && state.all_request[q.front()].state == RequestState::FINISHED) {
            q.pop();
        }
    }

    void ensureCloudInit(const SystemParams& params) {
        if (total_assigned_work.empty()) {
            total_assigned_work.assign(params.K, 0.0);
            cloud_load.clear();
            for (int k = 0; k < params.K; ++k) {
                cloud_load.insert({0.0, k});
            }
        }
    }

    int getOptimalCloudNode(const SystemState& state, const SystemParams& params, int r_id) {
        ensureCloudInit(params);
        auto it = cloud_load.begin();
        int best_cloud = it->second;
        double new_work = it->first + static_cast<double>(state.all_request[r_id].length_in);
        cloud_load.erase(it);
        cloud_load.insert({new_work, best_cloud});
        total_assigned_work[best_cloud] = new_work;
        return best_cloud;
    }

    bool isAnyCloudStarving(const SystemState& state, const SystemParams& params) {
        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE &&
                state.waiting_for_p_proc[k].empty() &&
                state.waiting_for_d_proc[k].empty()) {
                return true;
            }
        }
        return false;
    }

    void emitDPost(SystemState& state, std::string& out, int& count) {
        Batcher::pullBatch(state.waiting_for_d_post, cached_optimal_max, state, batch_buf);
        out.append("E D POST -1 ");
        Batcher::appendBatchStr(out, batch_buf);
        out.push_back('\n');
        count++;
        state.edge_server.setBusy();
    }

    void emitDPre(SystemState& state, std::string& out, int& count) {
        Batcher::pullBatch(state.waiting_for_d_pre, cached_optimal_max, state, batch_buf);
        out.append("E D PRE -1 ");
        Batcher::appendBatchStr(out, batch_buf);
        out.push_back('\n');
        count++;
        state.edge_server.setBusy();
    }

    void emitPPost(SystemState& state, std::string& out, int& count) {
        int r_id = state.waiting_for_p_post.front();
        state.waiting_for_p_post.pop();
        int cloud = state.all_request[r_id].assigned_cloud;
        out.append("E P POST ");
        appendIntToString(out, cloud);
        out.push_back(' ');
        appendIntToString(out, r_id);
        out.push_back('\n');
        count++;
        state.edge_server.setBusy();
    }

    void emitPPre(SystemState& state, const SystemParams& params, std::string& out, int& count) {
        int r_id = state.waiting_for_p_pre.front();
        state.waiting_for_p_pre.pop();
        int cloud = getOptimalCloudNode(state, params, r_id);
        state.all_request[r_id].assigned_cloud = cloud;
        out.append("E P PRE ");
        appendIntToString(out, cloud);
        out.push_back(' ');
        appendIntToString(out, r_id);
        out.push_back('\n');
        count++;
        state.edge_server.setBusy();
    }

public:
    void scheduleTasks(SystemState& state, const EventParser& parser, std::string& out, int& count) {
        count = 0;
        const SystemParams& params = parser.params;

        // Dynamic Initialization
        if (cached_optimal_max < 0) {
            if (params.w_c < 0.01) {
                current_strategy = StrategyMode::THROUGHPUT_EXTREME;
                cached_optimal_max = Batcher::computeMaxBatch(parser.task_time_table);
            } else if (params.w_tp > params.w_c) {
                current_strategy = StrategyMode::BALANCED;
                cached_optimal_max = Batcher::computeMaxBatch(parser.task_time_table);
            } else {
                current_strategy = StrategyMode::LATENCY_STRICT;
                cached_optimal_max = Batcher::computeOptimalMax(parser.task_time_table, params.SLO2);
            }
        }

        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        // --- Edge Server Scheduling ---
        if (state.edge_server.state == ServerState::FREE) {
            if (!state.waiting_for_d_post.empty()) {
                emitDPost(state, out, count);
            } else {
                if (current_strategy == StrategyMode::THROUGHPUT_EXTREME) {
                    // Massive Batch Accumulation: prefill completely before starting decode
                    if (!state.waiting_for_p_post.empty()) {
                        emitPPost(state, out, count);
                    } else if (!state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    } else if (!state.waiting_for_d_pre.empty()) {
                        emitDPre(state, out, count);
                    }
                } else {
                    // Latency / Balanced Mode: Feed the clouds efficiently
                    bool cloud_starving = isAnyCloudStarving(state, params);
                    if (!state.waiting_for_p_post.empty()) {
                        emitPPost(state, out, count);
                    } else if (cloud_starving && !state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    } else if (!state.waiting_for_d_pre.empty()) {
                        emitDPre(state, out, count);
                    } else if (!state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    }
                }
            }
        }

        // --- Cloud Server Scheduling ---
        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                if (!state.waiting_for_d_proc[k].empty()) {
                    Batcher::pullBatch(state.waiting_for_d_proc[k], cached_optimal_max, state, batch_buf);
                    out.push_back('C');
                    appendIntToString(out, k);
                    out.append(" D PROC ");
                    appendIntToString(out, k);
                    out.push_back(' ');
                    Batcher::appendBatchStr(out, batch_buf);
                    out.push_back('\n');
                    count++;
                    state.cloud_servers[k].setBusy();
                    
                } else if (!state.waiting_for_p_proc[k].empty()) {
                    int r_id = state.waiting_for_p_proc[k].front(); 
                    state.waiting_for_p_proc[k].pop();
                    
                    int ls = state.all_request[r_id].layers_completed;
                    // Strategy-aware chunking:
                    //   THROUGHPUT_EXTREME: chunk=2 under pressure, cap=16 without (proven best for test 19)
                    //   BALANCED/LATENCY:   chunk=8 under pressure, uncapped without
                    int pressure_chunk = (current_strategy == StrategyMode::THROUGHPUT_EXTREME) ? 2 : 8;
                    int no_pressure_cap = (current_strategy == StrategyMode::THROUGHPUT_EXTREME) ? 16 : 0;
                    int le = Chunker::getNextChunkEnd(ls, params, state.waiting_for_d_proc[k].size(), pressure_chunk, no_pressure_cap);
                    
                    out.push_back('C');
                    appendIntToString(out, k);
                    out.append(" P PROC ");
                    appendIntToString(out, ls);
                    out.push_back(' ');
                    appendIntToString(out, le);
                    out.push_back(' ');
                    appendIntToString(out, k);
                    out.push_back(' ');
                    appendIntToString(out, r_id);
                    out.push_back('\n');
                    count++;
                    state.cloud_servers[k].setBusy();
                }
            }
        }
    }
};
