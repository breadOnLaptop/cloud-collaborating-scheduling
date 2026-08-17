/**
 * @file advanced_scheduler.h
 * @brief Primary decision engine for intelligent task distribution.
 *        Implements pipeline-aware priority scheduling with O(log K) weighted
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
 * Edge priority (pipeline-aware):
 *   1. D_POST  — always first: completes a token (critical for TPOT)
 *   2. P_POST  — completes a prefill, reduces TDR, makes request decode-ready
 *   3. P_PRE   — only when a cloud server is idle: prevents pipeline starvation
 *   4. D_PRE   — batches decode requests for cloud processing
 *   5. P_PRE   — background prefill when no cloud is starving
 *
 * Cloud priority: D_PROC > P_PROC (decode-first to minimize token latency)
 *
 * This priority order prevents the edge from being monopolized by decode
 * operations, which would starve new requests and cause TDR to explode
 * on large inputs.
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
     *        Used to trigger P_PRE priority elevation to prevent pipeline starvation.
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

public:
    /**
     * @brief Evaluates all queues and emits assignment directives into an output buffer.
     */
    void scheduleTasks(SystemState& state, const EventParser& parser, std::string& out, int& count) {
        count = 0;
        const SystemParams& params = parser.params;

        if (cached_optimal_max < 0) {
            cached_optimal_max = Batcher::computeOptimalMax(parser.task_time_table, params.SLO2);
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

        // --- Edge server scheduling (pipeline-aware priority) ---
        if (state.edge_server.state == ServerState::FREE) {
            // Priority 1: D POST — completes a token, critical for both tp and TPOT
            if (!state.waiting_for_d_post.empty()) {
                Batcher::pullBatch(state.waiting_for_d_post, cached_optimal_max, state, batch_buf);
                out.append("E D POST -1 ");
                Batcher::appendBatchStr(out, batch_buf);
                out.push_back('\n');
                count++;
                state.edge_server.setBusy();

            // Priority 2: P POST — completes prefill, reduces TDR, makes request decode-ready
            } else if (!state.waiting_for_p_post.empty()) {
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

            // Priority 3: P PRE when clouds are starving — prevents pipeline starvation
            } else if (!state.waiting_for_p_pre.empty() && isAnyCloudStarving(state, params)) {
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

            // Priority 4: D PRE — starts a decode batch
            } else if (!state.waiting_for_d_pre.empty()) {
                Batcher::pullBatch(state.waiting_for_d_pre, cached_optimal_max, state, batch_buf);
                out.append("E D PRE -1 ");
                Batcher::appendBatchStr(out, batch_buf);
                out.push_back('\n');
                count++;
                state.edge_server.setBusy();

            // Priority 5: P PRE — background prefill when no cloud is starving
            } else if (!state.waiting_for_p_pre.empty()) {
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
