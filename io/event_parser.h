/**
 * @file event_parser.h
 * @brief Handles data ingestion and event parsing for the scheduling system.
 * @author Authored by: opt1mal
 */

#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <cstring>
#include <cstdlib>

#include "../state/system_state.h"

/**
 * @struct SystemParams
 * @brief Encapsulates system-wide configuration metrics.
 */
struct SystemParams {
  int K;                    // Number of cloud servers.
  double S;                 // Setup penalty time.
  double latency;           // Network latency.
  double bandwidth;         // Network bandwidth.
  int bytes_per_token;      // Data size per generated token.
  int num_layers;           // Total layers in the neural network model.
  double SLO1;              // Service Level Objective 1 constraint.
  double SLO2;              // Service Level Objective 2 constraint.
  double tp_UB;             // Throughput upper bound.
  double tp_base;           // Base throughput metric.
  double dist_base;         // Base distance cost metric.
  double w_tp;              // Weight for throughput in the scoring function.
  double w_c;               // Weight for cost in the scoring function.
};

/**
 * @struct TaskDurations
 * @brief Maps execution stage durations for a specific batch size.
 */
struct TaskDurations {
  double p_pre;   // Prefill pre-processing duration.
  double p_proc;  // Prefill processing duration.
  double p_post;  // Prefill post-processing duration.
  double d_pre;   // Decode pre-processing duration.
  double d_proc;  // Decode processing duration.
  double d_post;  // Decode post-processing duration.
};

class EventParser {
public:
  double current_time;                            // Current simulation timestamp.
  SystemParams params;                            // Global system parameters.
  std::unordered_map<int, TaskDurations> task_time_table;   // Table of execution times grouped by batch size.

  /**
   * @brief Reads the initial startup configuration parameters.
   */
  void readStartupConfig() {
    std::cin >> params.K >> params.S >> params.latency >> params.bandwidth
             >> params.bytes_per_token >> params.num_layers;
    std::cin >> params.SLO1 >> params.SLO2 >> params.tp_UB >> params.tp_base >> params.dist_base
             >> params.w_tp >> params.w_c;
  }

  /**
   * @brief Reads the task execution duration table from standard input.
   */
  void readTaskTimeTable() {
    int N;
    std::cin >> N;
    for(int i = 0; i < N; i++) {
      int batch_size;
      std::cin >> batch_size;
      TaskDurations td;
      std::cin >> td.p_pre >> td.p_proc >> td.p_post
               >> td.d_pre >> td.d_proc >> td.d_post;
      task_time_table[batch_size] = td;
    }
  }

  /**
   * @brief Processes the next event frame and mutates the provided system state.
   * @param state The global system state to mutate.
   * @return true if the simulation is ongoing, false if the END signal is received.
   */
  bool readNextFrame(SystemState& state) {
    // Read first token as char[] to handle both END sentinel and timestamp without heap alloc
    char ts_buf[32];
    if (!(std::cin >> ts_buf)) return false;
    if (ts_buf[0] == 'E') return false; // END signal

    current_time = std::strtod(ts_buf, nullptr);

    int event_count;
    std::cin >> event_count;

    // Fix 4: Stack-allocated char buffers for all short tokens — zero heap allocations
    char event_type[8];
    char server_name[16];
    char p_or_d[4];
    char step[8];

    for(int i = 0; i < event_count; i++) {
      std::cin >> event_type;

      if(event_type[0] == 'A') { // ARR
        int r_id, length_in;
        std::cin >> r_id >> length_in;

        Request new_req(r_id, length_in);
        if (r_id >= static_cast<int>(state.all_request.size())) {
          state.all_request.resize(r_id + 1);
        }
        state.all_request[r_id] = std::move(new_req);
        state.waiting_for_p_pre.push(r_id);

      } else if(event_type[0] == 'T') { // TDN
        std::cin >> server_name;

        if (server_name[0] == 'E') {
          state.edge_server.setFree();
        } else {
          // Fix 5: Parse cloud ID from char[] without substr or stoi
          int cid = 0;
          for (int j = 1; server_name[j]; j++) {
            cid = cid * 10 + (server_name[j] - '0');
          }
          state.cloud_servers[cid].setFree();
        }

        std::cin >> p_or_d >> step;
        
        int remote, ls, le, m, r_id, minus_one;
        double duration;

        if (p_or_d[0] == 'P') {
          if (step[1] == 'O') { // POST
            std::cin >> remote >> r_id >> duration;
            state.all_request[r_id].state = RequestState::READY_FOR_DECODE;
            state.waiting_for_d_pre.push(r_id);
          } else if (step[2] == 'E') { // PRE (step[2] distinguishes PRE from PROC)
            std::cin >> remote >> r_id >> duration;
          } else { // PROC
            std::cin >> ls >> le >> remote >> r_id >> duration;
            state.all_request[r_id].layers_completed = le;
            if (le < params.num_layers) {
              state.all_request[r_id].state = RequestState::READY_FOR_PREFILL;
              state.waiting_for_p_proc[remote].push(r_id);
            }
          }
        } else { // D
          if (step[1] == 'O') { // POST
            std::cin >> minus_one >> m;
            for(int j = 0; j < m; j++) { 
              std::cin >> r_id;
              state.all_request[r_id].state = RequestState::READY_FOR_DECODE;
              state.waiting_for_d_pre.push(r_id);
              state.all_request[r_id].length_out++;
            }
            std::cin >> duration;
          } else if (step[2] == 'E') { // PRE
            std::cin >> minus_one >> m;
            for(int j = 0; j < m; j++) { std::cin >> r_id; }
            std::cin >> duration;
          } else { // PROC
            std::cin >> remote >> m;
            for(int j = 0; j < m; j++) { std::cin >> r_id; }
            std::cin >> duration;
          }
        }

      } else if(event_type[0] == 'X') { // XDN
        char direction[8];
        char stage[8];
        std::cin >> direction;
        
        int remote, m; 
        long long size; 
        
        std::cin >> remote >> size >> stage >> m;

        for(int j = 0; j < m; j++) {
          int r_id;
          std::cin >> r_id;

          if (direction[0] == 'U') { // UP
            if (stage[0] == 'P') { // PRE
              state.all_request[r_id].state = RequestState::READY_FOR_PREFILL;
              state.waiting_for_p_proc[remote].push(r_id);
            } else { // DEC
              state.all_request[r_id].state = RequestState::READY_FOR_D_PROC;
              state.waiting_for_d_proc[remote].push(r_id);
            }
          } else { // DOWN
            if (stage[0] == 'P') { // PRE
              state.all_request[r_id].state = RequestState::READY_FOR_P_POST;
              state.waiting_for_p_post.push(r_id);
            } else { // DEC
              state.all_request[r_id].state = RequestState::READY_FOR_D_POST;
              state.waiting_for_d_post.push(r_id);
            }
          }
        }
      } else if(event_type[0] == 'F') { // FIN
        int r_id;
        std::cin >> r_id;
        state.all_request[r_id].state = RequestState::FINISHED;
      }
    }

    return true;
  }
};
