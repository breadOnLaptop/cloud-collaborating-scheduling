/**
 * @file system_state.h
 * @brief Centralized state management for the entire scheduling system.
 * @author Authored by: Peeyush Maurya
 */

#pragma once

#include <vector>
#include <queue>
#include "../entity/request.h"
#include "../entity/server.h"

/**
 * @struct SystemState
 * @brief Maintains the global memory, hardware status, and task queues.
 *
 * Provides a unified source of truth for the scheduler, containing the status
 * of all requests, the state of all compute nodes, and specialized queues
 * representing the bottlenecks at various stages of the computation pipeline.
 */
struct SystemState {
  std::vector<Request> all_request; // Registry of all requests managed by the system.

  Server edge_server;                   // The local edge compute node.
  std::vector<Server> cloud_servers;    // The remote cloud compute nodes.

  std::queue<int> waiting_for_p_pre;    // Queue for fresh arrivals awaiting routing.
  std::queue<int> waiting_for_p_post;   // Queue for prefilled requests awaiting post-processing.
  std::queue<int> waiting_for_d_pre;    // Queue for requests ready to begin a decode iteration.
  std::queue<int> waiting_for_d_post;   // Queue for requests awaiting token finalization.

  std::vector<std::queue<int>> waiting_for_p_proc;  // Queues per cloud server for heavy prefill tasks.
  std::vector<std::queue<int>> waiting_for_d_proc;  // Queues per cloud server for fast decode tasks.

  /**
   * @brief Constructs and initializes the SystemState.
   * @param cloud_num The total number of available cloud servers.
   */
  SystemState(const int& cloud_num) : edge_server(0, ServerType::EDGE) {
    for(int i = 0; i < cloud_num; i++) {
      cloud_servers.push_back(Server(i, ServerType::CLOUD));
      waiting_for_p_proc.push_back(std::queue<int>());
      waiting_for_d_proc.push_back(std::queue<int>());
    }
  }
};
