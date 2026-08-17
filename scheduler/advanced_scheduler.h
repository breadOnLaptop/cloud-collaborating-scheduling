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
    std::vector<std::string> scheduleTasks(SystemState& state, const EventParser& parser) {
        std::vector<std::string> assignments;
        const SystemParams& params = parser.params;

        // O(1) Amortized cleanup phase
        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        int optimal_max = Batcher::computeOptimalMax(parser.task_time_table, params.SLO2);

        if (state.edge_server.state == ServerState::FREE) {
            // Priority 1: D POST (Finish token quickly for SLO2)
            if (!state.waiting_for_d_post.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_post, optimal_max, state);
                std::string s; s.reserve(64);
                s.append("E D POST -1 ");
                s.append(Batcher::formatBatchStr(batch));
                assignments.push_back(s);
                state.edge_server.setBusy();
                
            // Priority 2: D PRE (Start token quickly for SLO2)
            } else if (!state.waiting_for_d_pre.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_pre, optimal_max, state);
                std::string s; s.reserve(64);
                s.append("E D PRE -1 ");
                s.append(Batcher::formatBatchStr(batch));
                assignments.push_back(s);
                state.edge_server.setBusy();
                
            // Priority 3: P POST (Data returning from Cloud)
            } else if (!state.waiting_for_p_post.empty()) {
                int r_id = state.waiting_for_p_post.front(); 
                state.waiting_for_p_post.pop();
                
                int cloud = state.all_request[r_id].assigned_cloud;
                std::string s; s.reserve(32);
                s.append("E P POST ");
                appendIntToString(s, cloud);
                s.push_back(' ');
                appendIntToString(s, r_id);
                assignments.push_back(s);
                state.edge_server.setBusy();
                
            // Priority 4: P PRE (New data arriving at Cloud)
            } else if (!state.waiting_for_p_pre.empty()) {
                int r_id = state.waiting_for_p_pre.front(); 
                state.waiting_for_p_pre.pop();
                
                // O(log K) weighted assignment
                int cloud = getOptimalCloudNode(state, params, r_id);
                state.all_request[r_id].assigned_cloud = cloud;
                
                std::string s; s.reserve(32);
                s.append("E P PRE ");
                appendIntToString(s, cloud);
                s.push_back(' ');
                appendIntToString(s, r_id);
                assignments.push_back(s);
                state.edge_server.setBusy();
            }
        }

        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                // Yield to D_PROC decodes natively for token generation SLO
                if (!state.waiting_for_d_proc[k].empty()) {
                    std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_proc[k], optimal_max, state);
                    std::string s; s.reserve(64);
                    s.append("C");
                    appendIntToString(s, k);
                    s.append(" D PROC ");
                    appendIntToString(s, k);
                    s.push_back(' ');
                    s.append(Batcher::formatBatchStr(batch));
                    assignments.push_back(s);
                    state.cloud_servers[k].setBusy();
                    
                } else if (!state.waiting_for_p_proc[k].empty()) {
                    int r_id = state.waiting_for_p_proc[k].front(); 
                    state.waiting_for_p_proc[k].pop();
                    
                    int ls = state.all_request[r_id].layers_completed;
                    int le = Chunker::getNextChunkEnd(ls, params, state.waiting_for_d_proc[k].size());
                    
                    std::string s; s.reserve(64);
                    s.append("C");
                    appendIntToString(s, k);
                    s.append(" P PROC ");
                    appendIntToString(s, ls);
                    s.push_back(' ');
                    appendIntToString(s, le);
                    s.push_back(' ');
                    appendIntToString(s, k);
                    s.push_back(' ');
                    appendIntToString(s, r_id);
                    assignments.push_back(s);
                    state.cloud_servers[k].setBusy();
                }
            }
        }

        return assignments;
    }
};
