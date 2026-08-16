#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <map>

#include "state/system_state.h"

struct SystemParams {
  int K;
  double S, latency, bandwidth;
  int bytes_per_token, num_layers;
  double SLO1, SLO2, tp_UB, tp_base, dist_base, w_tp, w_c;
};

struct TaskDurations {
  double p_pre, p_proc, p_post, d_pre, d_proc, d_post;
};

struct TaskLog {
  double current_time;
  std::string server_name;
  std::string p_or_d;
  std::string step;
  int remote = -1;
  int ls = -1, le = -1;
  int m = 0;
  std::vector<int> r_ids;
  double duration = 0.0;
};

class EventParser {
  public:
  double current_time;
  SystemParams params;
  std::map<int, TaskDurations> task_time_table;
  std::vector<TaskLog> task_history;

  void readStartupConfig() {
    std::cin >> params.K >> params.S >> params.latency >> params.bandwidth
             >> params.bytes_per_token >> params.num_layers;
    std::cin >> params.SLO1 >> params.SLO2 >> params.tp_UB >> params.tp_base >> params.dist_base
             >> params.w_tp >> params.w_c;
  }

  void readTaskTimeTable() {
    int N;
    std::cin >> N;
    for(int i{}; i < N; i++) {
      int batch_size;
      std::cin >> batch_size;
      TaskDurations td;
      std::cin >> td.p_pre >> td.p_proc >> td.p_post
               >> td.d_pre >> td.d_proc >> td.d_post;

      task_time_table[batch_size] = td;
    }
  }

  bool readNextFrame(SystemState& state) {
    std::string token;
    std::cin >> token;

    if(token == "END" || std::cin.eof()) return false;

    current_time = std::stod(token);

    int event_count;
    std::cin >> event_count;

    for(int i{}; i < event_count; i++) {
      std::string event_type;
      std::cin >> event_type;

      if(event_type == "ARR") {
        int r_id, length_in;
        std::cin >> r_id >> length_in;

        Request new_req(r_id, length_in);
        state.all_request.push_back(new_req);
        state.waiting_for_p_pre.push(r_id);

      } else if(event_type == "TDN") {
        std::string server_name;
        std::cin >> server_name;

        if (server_name == "E") {
          state.edge_server.setFree();
        } else {
          int cid = std::stoi(server_name.substr(1));
          state.cloud_servers[cid].setFree();
        }

        std::string p_or_d, step;
        std::cin >> p_or_d >> step;

        TaskLog log;
        log.current_time = current_time;
        log.server_name = server_name;
        log.p_or_d = p_or_d;
        log.step = step;

        if (p_or_d == "P") {
          if (step == "PRE") {
            int r_id;
            std::cin >> log.remote >> r_id >> log.duration;
            log.r_ids.push_back(r_id);
          } else if (step == "PROC") {
            int r_id;
            std::cin >> log.ls >> log.le >> log.remote >> r_id >> log.duration;
            log.r_ids.push_back(r_id);
            
            state.all_request[r_id].layers_completed = log.le;
            if (log.le < params.num_layers) {
              state.all_request[r_id].state = RequestState::READY_FOR_PREFILL;
              state.waiting_for_p_proc[log.remote].push(r_id);
            }
          } else if (step == "POST") {
            int r_id;
            std::cin >> log.remote >> r_id >> log.duration;
            log.r_ids.push_back(r_id);
            
            state.all_request[r_id].state = RequestState::READY_FOR_DECODE;
            state.waiting_for_d_pre.push(r_id);
          }
        } else if (p_or_d == "D") {
          if (step == "PRE") {
            int minus_one; std::cin >> minus_one >> log.m;
            for(int j{}; j < log.m; j++) { int r_id; std::cin >> r_id; log.r_ids.push_back(r_id); }
            std::cin >> log.duration;
          } else if (step == "PROC") {
            std::cin >> log.remote >> log.m;
            for(int j{}; j < log.m; j++) { int r_id; std::cin >> r_id; log.r_ids.push_back(r_id); }
            std::cin >> log.duration;
          } else if (step == "POST") {
            int minus_one; std::cin >> minus_one >> log.m;
            for(int j{}; j < log.m; j++) { 
              int r_id; std::cin >> r_id; log.r_ids.push_back(r_id);
              
              state.all_request[r_id].state = RequestState::READY_FOR_DECODE;
              state.waiting_for_d_pre.push(r_id);
              state.all_request[r_id].length_out++;
            }
            std::cin >> log.duration;
          }
        }
        task_history.push_back(log);

      } else if(event_type == "XDN") {
        std::string direction;
        std::cin >> direction;
        
        int remote, m; 
        long long size; 
        std::string stage;
        
        std::cin >> remote >> size >> stage >> m;

        for(int j{}; j < m; j++) {
          int r_id;
          std::cin >> r_id;

          if (direction == "UP" && stage == "PRE") {
            state.all_request[r_id].state = RequestState::READY_FOR_PREFILL;
            state.waiting_for_p_proc[remote].push(r_id);
          } 
          else if (direction == "DOWN" && stage == "PRE") {
            state.all_request[r_id].state = RequestState::READY_FOR_P_POST;
            state.waiting_for_p_post.push(r_id);
          } 
          else if (direction == "UP" && stage == "DEC") {
            state.all_request[r_id].state = RequestState::READY_FOR_D_PROC;
            state.waiting_for_d_proc[remote].push(r_id);
          } 
          else if (direction == "DOWN" && stage == "DEC") {
            state.all_request[r_id].state = RequestState::READY_FOR_D_POST;
            state.waiting_for_d_post.push(r_id);
          }
        }
      } else if(event_type == "FIN") {
        int r_id;
        std::cin >> r_id;

        state.all_request[r_id].state = RequestState::FINISHED;
      }
    }

    return true;
  }
};
