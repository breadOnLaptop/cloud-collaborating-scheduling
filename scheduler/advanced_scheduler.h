/**
 * @file advanced_scheduler.h
 * @brief Primary decision engine for intelligent task distribution.
 *        Includes Min-Heap usage for efficient queue state lookups. Refer to scheduler/baseline_scheduler.h for the update reference. 
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

class AdvancedScheduler {
private:
    std::vector<double> total_assigned_work;
    std::set<std::pair<double, int>> cloud_load;
    int cached_optimal_max = -1; // Computed once, reused forever
    std::vector<int> batch_buf;  // Reusable scratch buffer for batch pulls

    // Amortized O(1) queue cleaning to strictly avoid sorting overhead
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

    // Weighted load tracking using O(log K) set
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

public:
    // Writes all assignments directly into a single output buffer — zero vector/string allocations
    void scheduleTasks(SystemState& state, const EventParser& parser, std::string& out, int& count) {
        count = 0;
        const SystemParams& params = parser.params;

        // Cache optimal_max once globally — task_time_table never changes after startup
        if (cached_optimal_max < 0) {
            cached_optimal_max = Batcher::computeOptimalMax(parser.task_time_table, params.SLO2);
        }

        // O(1) Amortized cleanup phase
        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        if (state.edge_server.state == ServerState::FREE) {
            // Priority 1: D POST (Finish token quickly for SLO2)
            if (!state.waiting_for_d_post.empty()) {
                Batcher::pullBatch(state.waiting_for_d_post, cached_optimal_max, state, batch_buf);
                out.append("E D POST -1 ");
                Batcher::appendBatchStr(out, batch_buf);
                out.push_back('\n');
                count++;
                state.edge_server.setBusy();
                
            // Priority 2: D PRE (Start token quickly for SLO2)
            } else if (!state.waiting_for_d_pre.empty()) {
                Batcher::pullBatch(state.waiting_for_d_pre, cached_optimal_max, state, batch_buf);
                out.append("E D PRE -1 ");
                Batcher::appendBatchStr(out, batch_buf);
                out.push_back('\n');
                count++;
                state.edge_server.setBusy();
                
            // Priority 3: P POST (Data returning from Cloud)
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
                
            // Priority 4: P PRE (New data arriving at Cloud)
            } else if (!state.waiting_for_p_pre.empty()) {
                int r_id = state.waiting_for_p_pre.front(); 
                state.waiting_for_p_pre.pop();
                
                // O(log K) weighted assignment
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

        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                // Yield to D_PROC decodes natively for token generation SLO
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
