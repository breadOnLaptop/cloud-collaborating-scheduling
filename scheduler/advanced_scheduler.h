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
#include <map>
#include <queue>
#include "../state/system_state.h"
#include "../io/event_parser.h"
#include "chunker.h"
#include "batcher.h"

class AdvancedScheduler {
private:
    std::vector<double> total_assigned_work;

    // Amortized O(1) queue cleaning coupled with SJF state space reduction
    void cleanAndSortQueueSJF(std::queue<int>& q, const SystemState& state) {
        while (!q.empty() && state.all_request[q.front()].state == RequestState::FINISHED) {
            q.pop();
        }
        
        if (q.size() > 1) {
            std::vector<int> items;
            while (!q.empty()) {
                int id = q.front();
                q.pop();
                if (state.all_request[id].state != RequestState::FINISHED) {
                    items.push_back(id);
                }
            }
            // State space reduction: Shortest Job First (SJF) drastically minimizes mean_tdr
            std::sort(items.begin(), items.end(), [&state](int a, int b) {
                return state.all_request[a].length_in < state.all_request[b].length_in;
            });
            for (int id : items) {
                q.push(id);
            }
        }
    }

    // Weighted load tracking for perfect Cloud balance
    int getOptimalCloudNode(const SystemState& state, const SystemParams& params, int r_id) {
        if (total_assigned_work.empty()) {
            total_assigned_work.assign(params.K, 0.0);
        }
        
        int best_cloud = 0;
        double min_work = 1e18;
        for (int k = 0; k < params.K; ++k) {
            if (total_assigned_work[k] < min_work) {
                min_work = total_assigned_work[k];
                best_cloud = k;
            }
        }
        
        // Accumulate work natively using token length to avoid O(N) looping
        total_assigned_work[best_cloud] += state.all_request[r_id].length_in;
        return best_cloud;
    }

public:
    std::vector<std::string> scheduleTasks(SystemState& state, const EventParser& parser) {
        std::vector<std::string> assignments;
        const SystemParams& params = parser.params;

        // O(1) Amortized cleanup phase + SJF Sort
        cleanAndSortQueueSJF(state.waiting_for_p_pre, state);
        cleanAndSortQueueSJF(state.waiting_for_p_post, state);
        cleanAndSortQueueSJF(state.waiting_for_d_pre, state);
        cleanAndSortQueueSJF(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanAndSortQueueSJF(state.waiting_for_p_proc[k], state);
            cleanAndSortQueueSJF(state.waiting_for_d_proc[k], state);
        }

        if (state.edge_server.state == ServerState::FREE) {
            // Priority 1: D POST (Finish token quickly for SLO2)
            if (!state.waiting_for_d_post.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_post, parser.task_time_table, params.SLO2, state);
                assignments.push_back("E D POST -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            // Priority 2: D PRE (Start token quickly for SLO2)
            } else if (!state.waiting_for_d_pre.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_pre, parser.task_time_table, params.SLO2, state);
                assignments.push_back("E D PRE -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            // Priority 3: P POST (Data returning from Cloud)
            } else if (!state.waiting_for_p_post.empty()) {
                int r_id = state.waiting_for_p_post.front(); 
                state.waiting_for_p_post.pop();
                
                int cloud = state.all_request[r_id].assigned_cloud;
                assignments.push_back("E P POST " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
                
            // Priority 4: P PRE (New data arriving at Cloud)
            } else if (!state.waiting_for_p_pre.empty()) {
                int r_id = state.waiting_for_p_pre.front(); 
                state.waiting_for_p_pre.pop();
                
                int cloud = getOptimalCloudNode(state, params, r_id);
                state.all_request[r_id].assigned_cloud = cloud;
                
                assignments.push_back("E P PRE " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
            }
        }

        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                // Yield to D_PROC decodes natively for token generation SLO
                if (!state.waiting_for_d_proc[k].empty()) {
                    std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_proc[k], parser.task_time_table, params.SLO2, state);
                    assignments.push_back("C" + std::to_string(k) + " D PROC " + std::to_string(k) + " " + Batcher::formatBatchStr(batch));
                    state.cloud_servers[k].setBusy();
                    
                } else if (!state.waiting_for_p_proc[k].empty()) {
                    int r_id = state.waiting_for_p_proc[k].front(); 
                    state.waiting_for_p_proc[k].pop();
                    
                    int ls = state.all_request[r_id].layers_completed;
                    int le = Chunker::getNextChunkEnd(ls, params, state.waiting_for_d_proc[k].size());
                    
                    assignments.push_back("C" + std::to_string(k) + " P PROC " + std::to_string(ls) + " " + std::to_string(le) + " " + std::to_string(k) + " " + std::to_string(r_id));
                    state.cloud_servers[k].setBusy();
                }
            }
        }

        return assignments;
    }
};
