/**
 * @file advanced_scheduler.h
 * @brief Primary decision engine for intelligent task distribution.
 *        Implements weight-adaptive priority scheduling with O(log K) weighted
 *        cloud load balancing and mode-specific batch/chunk strategies.
 *        Refer to scheduler/baseline_scheduler.h for the baseline reference.
 *        Submission Link for baseline_scheduler.h: https://codeforces.com/contest/2251/submission/387277746 (Link will be usable after the contest ends)
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

/**
 * @class AdvancedScheduler
 * @brief Orchestrates task assignment across edge and cloud servers.
 *
 * Two scheduling modes selected by comparing w_tp and w_c:
 *
 * Throughput-focused (w_tp > w_c):
 *   Edge: D_POST > D_PRE > P_POST > P_PRE (pure decode-first)
 *   Batch: max size from table (TPOT irrelevant, maximize tokens per edge op)
 *   Chunks: all remaining layers at once (eliminate setup overhead)
 *
 * Latency-focused (w_c >= w_tp):
 *   Edge: D_POST > P_POST > P_PRE (cloud idle) > D_PRE > P_PRE
 *   Batch: SLO2-bounded (protect TPOT)
 *   Chunks: adaptive yielding to decode pressure
 *
 * Cloud priority is always: D_PROC > P_PROC (decode-first).
 */
class AdvancedScheduler {
private:
    std::vector<double> total_assigned_work;       // Cumulative work assigned per cloud node.
    std::set<std::pair<double, int>> cloud_load;   // Sorted (work, cloud_id) for O(log K) min-pick.
    int cached_optimal_max = -1;                   // Batch limit, computed once at startup.
    bool throughput_mode = false;                   // True when w_tp > w_c.
    std::vector<int> batch_buf;                    // Reusable scratch buffer for batch extraction.

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

    // --- Edge assignment emitters ---

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

        // One-time initialization: select mode and batch strategy
        if (cached_optimal_max < 0) {
            throughput_mode = (params.w_tp > params.w_c);
            if (throughput_mode) {
                // Uncapped batch: max amortization, TPOT irrelevant
                cached_optimal_max = Batcher::computeMaxBatch(parser.task_time_table);
            } else {
                // SLO2-bounded batch: protect TPOT
                cached_optimal_max = Batcher::computeOptimalMax(parser.task_time_table, params.SLO2, params.S);
            }
        }

        // Amortized O(1) cleanup
        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        // --- Edge server scheduling ---
        if (state.edge_server.state == ServerState::FREE) {
            if (!state.waiting_for_d_post.empty()) {
                // D POST always first — produces tokens
                emitDPost(state, out, count);

            } else if (throughput_mode) {
                // Throughput: pure decode-first, no cloud starvation elevation
                if (!state.waiting_for_d_pre.empty()) {
                    emitDPre(state, out, count);
                } else if (!state.waiting_for_p_post.empty()) {
                    emitPPost(state, out, count);
                } else if (!state.waiting_for_p_pre.empty()) {
                    emitPPre(state, params, out, count);
                }

            } else {
                // Latency: prefill-aware priority with cloud starvation feeding
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

        // --- Cloud server scheduling (decode-first, mode-adaptive chunking) ---
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
                    int le;
                    if (throughput_mode) {
                        // All remaining layers at once — zero extra setup penalties
                        le = params.num_layers;
                    } else {
                        // Adaptive chunking — yield to decode pressure
                        le = Chunker::getNextChunkEnd(ls, params, state.waiting_for_d_proc[k].size());
                    }
                    
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
