#pragma once

#include <vector>
#include <queue>

#include "entity/request.h"
#include "entity/server.h"

struct SystemState {
  std::vector<Request> all_request;

  Server edge_server;
  std::vector<Server> cloud_servers;

  std::queue<int> waiting_for_p_pre;
  std::queue<int> waiting_for_p_post;
  std::queue<int> waiting_for_d_pre;
  std::queue<int> waiting_for_d_post;

  std::vector<std::queue<int>> waiting_for_p_proc;
  std::vector<std::queue<int>> waiting_for_d_proc;

  SystemState(const int& cloud_num) : edge_server(0, ServerType::EDGE) {
    for(int i{}; i < cloud_num; i++) {
      cloud_servers.push_back(Server(i, ServerType::CLOUD));
      waiting_for_p_proc.push_back(std::queue<int>());
      waiting_for_d_proc.push_back(std::queue<int>());
    }
  };
};
