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

/**
 * @class AdvancedScheduler
 * @brief Orchestrates queue management and task dispatch mechanisms.
 *
 * Evaluates real-time state metrics to execute queue-aware routing,
 * dynamic batch sizing, and adaptive layer chunking algorithms across
 * distributed edge and cloud hardware topologies.
 */
class AdvancedScheduler {
private:
    /**
     * @brief Purges finalized requests from active processing queues.
     * @param q The target queue for sanitation.
     * @param state The global state object containing request lifecycle status.
     */
    void cleanQueue(std::queue<int>& q, const SystemState& state) {
        int sz = q.size();
        for (int i = 0; i < sz; ++i) {
            int id = q.front();
            q.pop();
            if (state.all_request[id].state != RequestState::FINISHED) {
                q.push(id);
            }
        }
    }

    /**
     * @brief Identifies the optimal cloud node based on current systemic load using a Min-Heap.
     * @param state The global state object.
     * @param params System parameters defining topology constraints.
     * @return The identifier of the least saturated cloud server.
     */
    int getOptimalCloudNode(const SystemState& state, const SystemParams& params) {
        // Min-Heap usage for efficient queue state lookups
        // Pair structure: {load_score, cloud_id}
        std::priority_queue<
            std::pair<size_t, int>, 
            std::vector<std::pair<size_t, int>>, 
            std::greater<std::pair<size_t, int>>
        > min_heap;
        
        for (int k = 0; k < params.K; ++k) {
            size_t load = state.waiting_for_p_proc[k].size() * 10 + state.waiting_for_d_proc[k].size();
            min_heap.push({load, k});
        }
        
        return min_heap.top().second;
    }

public:
    /**
     * @brief Evaluates active queues to formulate the subsequent execution matrix.
     * @param state The global state containing queue metrics.
     * @param parser The event parser containing configuration and timing tables.
     * @return A collection of formatted assignment directives.
     */
    std::vector<std::string> scheduleTasks(SystemState& state, const EventParser& parser) {
        std::vector<std::string> assignments;
        const SystemParams& params = parser.params;

        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        if (state.edge_server.state == ServerState::FREE) {
            if (!state.waiting_for_p_post.empty()) {
                int r_id = state.waiting_for_p_post.front(); 
                state.waiting_for_p_post.pop();
                
                int cloud = state.all_request[r_id].assigned_cloud;
                assignments.push_back("E P POST " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_d_post.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_post, parser.task_time_table);
                assignments.push_back("E D POST -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_d_pre.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_pre, parser.task_time_table);
                assignments.push_back("E D PRE -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_p_pre.empty()) {
                int r_id = state.waiting_for_p_pre.front(); 
                state.waiting_for_p_pre.pop();
                
                int cloud = getOptimalCloudNode(state, params);
                state.all_request[r_id].assigned_cloud = cloud;
                
                assignments.push_back("E P PRE " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
            }
        }

        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                if (!state.waiting_for_d_proc[k].empty()) {
                    std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_proc[k], parser.task_time_table);
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
