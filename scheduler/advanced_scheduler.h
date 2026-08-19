/**
 * @file advanced_scheduler.h
 * @brief Primary decision engine for intelligent task distribution.
 *        Implements weight-adaptive priority scheduling with O(log K) weighted
 *        cloud load balancing and adaptive prefill chunking.
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
 * The edge scheduling priority adapts to the test's scoring weights (w_tp, w_c):
 *
 * Latency-focused (w_c >= w_tp):
 *   D_POST > P_POST > P_PRE (cloud idle) > D_PRE > P_PRE
 *   Minimizes TDR by completing prefills promptly and feeding idle clouds.
 *
 * Throughput-focused (w_tp > w_c):
 *   D_POST > P_PRE (cloud idle) > D_PRE > P_POST > P_PRE
 *   Maximizes token rate by keeping the decode pipeline flowing and
 *   preventing cloud starvation (idle cloud = wasted throughput).
 *
 * Cloud priority is always: D_PROC > P_PROC (decode-first).
 */
class AdvancedScheduler {
private:
    std::vector<double> total_assigned_work;       // Cumulative work assigned per cloud node.
    std::set<std::pair<double, int>> cloud_load;   // Sorted (work, cloud_id) for O(log K) min-pick.
    int cached_optimal_max = -1;                   // SLO2-bounded batch limit, computed once at startup.
    std::vector<int> batch_buf;                    // Reusable scratch buffer for batch extraction.

    /**
     * @brief Pops finished requests from the front of a queue.
     *        Amortized O(1) per call — only pops consecutive finished entries.
     */
    void cleanQueue(std::queue<int>& q, const SystemState& state) {
        while (!q.empty() && state.all_request[q.front()].state == RequestState::FINISHED) {
            q.pop();
        }
    }

    /**
     * @brief Lazily initializes cloud load tracking structures on first use.
     */
    void ensureCloudInit(const SystemParams& params) {
        if (total_assigned_work.empty()) {
            total_assigned_work.assign(params.K, 0.0);
            cloud_load.clear();
            for (int k = 0; k < params.K; ++k) {
                cloud_load.insert({0.0, k});
            }
        }
    }

    /**
     * @brief Selects the least-loaded cloud server and assigns work to it.
     *        O(log K) via sorted set extraction and reinsertion.
     */
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

    /**
     * @brief Checks if any cloud server is idle with no queued work.
     *        An idle cloud is wasted capacity — triggers P_PRE elevation.
     */
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

    // --- Helper methods to emit assignment directives ---

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
    /**
     * @brief Evaluates all queues and emits assignment directives into an output buffer.
     *        Edge priority adapts based on scoring weights w_tp and w_c.
     */
    void scheduleTasks(SystemState& state, const EventParser& parser, std::string& out, int& count) {
        count = 0;
        const SystemParams& params = parser.params;

        if (cached_optimal_max < 0) {
            cached_optimal_max = Batcher::computeOptimalMax(parser.task_time_table, params.SLO2, params.S);
        }

        // Amortized O(1) cleanup: pop finished requests from queue heads
        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        // --- Edge server scheduling (weight-adaptive priority) ---
        if (state.edge_server.state == ServerState::FREE) {
            // D POST always first — directly produces tokens
            if (!state.waiting_for_d_post.empty()) {
                emitDPost(state, out, count);

            } else {
                bool cloud_starving = isAnyCloudStarving(state, params);

                if (params.w_c >= params.w_tp) {
                    // Latency-focused: minimize TDR by completing prefills promptly
                    if (!state.waiting_for_p_post.empty()) {
                        emitPPost(state, out, count);
                    } else if (cloud_starving && !state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    } else if (!state.waiting_for_d_pre.empty()) {
                        emitDPre(state, out, count);
                    } else if (!state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    }
                } else {
                    // Throughput-focused: keep decode pipeline flowing, prevent cloud idle
                    if (cloud_starving && !state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    } else if (!state.waiting_for_d_pre.empty()) {
                        emitDPre(state, out, count);
                    } else if (!state.waiting_for_p_post.empty()) {
                        emitPPost(state, out, count);
                    } else if (!state.waiting_for_p_pre.empty()) {
                        emitPPre(state, params, out, count);
                    }
                }
            }
        }

        // --- Cloud server scheduling (per-server, decode-first) ---
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
                    int le = Chunker::getNextChunkEnd(ls, params, state.waiting_for_d_proc[k].size());
                    
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
