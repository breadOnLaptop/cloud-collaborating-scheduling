#pragma once

#include <vector>
#include <queue>
#include "../entity/request.h"
#include "../entity/server.h"

struct SystemState {
  // Set of Requests
  std::vector<Request> all_request;

  // Hardware
  Server edge_server;
  std::vector<Server> cloud_servers;

  // EDGE queues: LOCAL computer
  // Holds request IDs waiting for edge computer (processed once per request)
  std::queue<int> waiting_for_p_pre;    // Fresh arrivals waiting to be routed
  std::queue<int> waiting_for_p_post;   // Prefilled requests waiting for P POST
  std::queue<int> waiting_for_d_pre;    // Prefilled requests waiting to start a decode loop
  std::queue<int> waiting_for_d_post;   // Processed requests waiting to finalized

  // CLOUD queues: REMOTE computer(s)
  // Each REMOTE computers are locked while processing. Thus, needs to be handled independently.
  std::vector<std::queue<int>> waiting_for_p_proc;  // heavy prefill tasks
  std::vector<std::queue<int>> waiting_for_d_proc;  // Fast decode tasks

  SystemState(const int& cloud_num) : edge_server(0, ServerType::EDGE) {
    // Initialize K servers
    for(int i{}; i < cloud_num; i++) {
      cloud_servers.push_back(Server(i, ServerType::CLOUD));

      // Initialize CLOUD queues
      waiting_for_p_proc.push_back(std::queue<int>());
      waiting_for_d_proc.push_back(std::queue<int>());
    }
  };
};
