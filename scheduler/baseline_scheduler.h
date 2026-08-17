/**
 * @file baseline_scheduler.h
 * @brief Baseline decision engine for task distribution.
 * @author Authored by: opt1mal
 */

#pragma once

#include <vector>
#include <string>
#include <map>
#include "../state/system_state.h"
#include "../io/event_parser.h"
#include "chunker.h"
#include "batcher.h"

/**
 * @class BaselineScheduler
 * @brief Provides naive scheduling implementation for baseline evaluation.
 *
 * Utilizes round-robin routing, static chunking, and hardcoded batch sizes.
 */
class BaselineScheduler {
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

public:
    /**
     * @brief Evaluates active queues to formulate the subsequent execution matrix.
     * @param state The global state containing queue metrics.
     * @param params System parameters defining topology constraints.
     * @return A collection of formatted assignment directives.
     */
    std::vector<std::string> scheduleTasks(SystemState& state, const SystemParams& params) {
        std::vector<std::string> assignments;

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
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_post, std::map<int, TaskDurations>());
                assignments.push_back("E D POST -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_d_pre.empty()) {
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_pre, std::map<int, TaskDurations>());
                assignments.push_back("E D PRE -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_p_pre.empty()) {
                int r_id = state.waiting_for_p_pre.front(); 
                state.waiting_for_p_pre.pop();
                
                int cloud = r_id % params.K; 
                state.all_request[r_id].assigned_cloud = cloud;
                
                assignments.push_back("E P PRE " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
            }
        }

        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                if (!state.waiting_for_d_proc[k].empty()) {
                    std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_proc[k], std::map<int, TaskDurations>());
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
