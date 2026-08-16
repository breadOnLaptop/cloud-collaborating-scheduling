#pragma once

#include <iostream>
#include <vector>
#include <string>
#include "../state/system_state.h"
#include "../io/event_parser.h"
#include "chunker.h"
#include "batcher.h"

class BaselineScheduler {
private:
    // Helper to remove any finished requests from a queue
    void cleanQueue(std::queue<int>& q, const SystemState& state) {
        int sz = q.size();
        for (int i = 0; i < sz; ++i) {
            int id = q.front();
            q.pop();
            // Only keep it if it's not finished!
            if (state.all_request[id].state != RequestState::FINISHED) {
                q.push(id);
            }
        }
    }

public:
    // This function will be called once per frame
    std::vector<std::string> scheduleTasks(SystemState& state, const SystemParams& params) {
        std::vector<std::string> assignments;

        // 0. CLEANUP
        // The FIN event arrives at the same time as the final D POST TDN.
        // Because of the TDN, we aggressively pushed the request back into waiting_for_d_pre.
        // We must remove it now before we accidentally assign it to a server!
        cleanQueue(state.waiting_for_p_pre, state);
        cleanQueue(state.waiting_for_p_post, state);
        cleanQueue(state.waiting_for_d_pre, state);
        cleanQueue(state.waiting_for_d_post, state);
        for(int k = 0; k < params.K; ++k) {
            cleanQueue(state.waiting_for_p_proc[k], state);
            cleanQueue(state.waiting_for_d_proc[k], state);
        }

        // ---------------------------------------------------------
        // 1. THE EDGE COMPUTER SCHEDULER
        // ---------------------------------------------------------
        // The Edge can only do ONE thing at a time. 
        if (state.edge_server.state == ServerState::FREE) {
            
            if (!state.waiting_for_p_post.empty()) {
                // Highest Priority: Finalize prefill so they can start decoding
                int r_id = state.waiting_for_p_post.front(); 
                state.waiting_for_p_post.pop();
                
                int cloud = state.all_request[r_id].assigned_cloud;
                assignments.push_back("E P POST " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_d_post.empty()) {
                // Priority 2: Finalize tokens! 
                // Optimization: Batch up to 16 ready requests at once!
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_post, 16);
                assignments.push_back("E D POST -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_d_pre.empty()) {
                // Priority 3: Prepare next token loop
                // Optimization: Batch up to 16 ready requests at once!
                std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_pre, 16);
                assignments.push_back("E D PRE -1 " + Batcher::formatBatchStr(batch));
                state.edge_server.setBusy();
                
            } else if (!state.waiting_for_p_pre.empty()) {
                // Lowest Priority: Start brand new requests
                int r_id = state.waiting_for_p_pre.front(); 
                state.waiting_for_p_pre.pop();
                
                // Baseline Load Balancer: Round Robin!
                int cloud = r_id % params.K; 
                state.all_request[r_id].assigned_cloud = cloud;
                
                assignments.push_back("E P PRE " + std::to_string(cloud) + " " + std::to_string(r_id));
                state.edge_server.setBusy();
            }
        }

        // ---------------------------------------------------------
        // 2. THE CLOUD COMPUTERS SCHEDULER
        // ---------------------------------------------------------
        // We must check every single cloud independently.
        for (int k = 0; k < params.K; ++k) {
            if (state.cloud_servers[k].state == ServerState::FREE) {
                
                if (!state.waiting_for_d_proc[k].empty()) {
                    // Priority 1: Fast Decode loops
                    // Optimization: Batch up to 16 ready requests at once!
                    std::vector<int> batch = Batcher::pullBatch(state.waiting_for_d_proc[k], 16);
                    assignments.push_back("C" + std::to_string(k) + " D PROC " + std::to_string(k) + " " + Batcher::formatBatchStr(batch));
                    state.cloud_servers[k].setBusy();
                    
                } else if (!state.waiting_for_p_proc[k].empty()) {
                    // Priority 2: Heavy Prefill Tasks
                    int r_id = state.waiting_for_p_proc[k].front(); 
                    state.waiting_for_p_proc[k].pop();
                    
                    // Optimization: Chunking! Get a smart slice of layers instead of doing all of them.
                    int ls = state.all_request[r_id].layers_completed;
                    int le = Chunker::getNextChunkEnd(ls, params);
                    
                    assignments.push_back("C" + std::to_string(k) + " P PROC " + std::to_string(ls) + " " + std::to_string(le) + " " + std::to_string(k) + " " + std::to_string(r_id));
                    state.cloud_servers[k].setBusy();
                }
            }
        }

        return assignments;
    }
};
