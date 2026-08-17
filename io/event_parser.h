/**
 * @file event_parser.h
 * @brief Handles data ingestion and event parsing for the scheduling system.
 * @author Authored by: opt1mal
 */

#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <map>

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

/**
 * @struct TaskLog
 * @brief Records the execution history of a completed task.
 */
struct TaskLog {
  double current_time;      // Timestamp of the event.
  std::string server_name;  // Identifier of the executing server.
  std::string p_or_d;       // Phase identifier (Prefill or Decode).
  std::string step;         // Sub-step identifier (PRE, PROC, POST).
  int remote = -1;          // Remote server index, if applicable.
  int ls = -1;              // Starting layer index.
  int le = -1;              // Ending layer index.
  int m = 0;                // Number of requests in the batch.
  std::vector<int> r_ids;   // Identifiers of requests within the batch.
  double duration = 0.0;    // Recorded duration of the task.
};

/**
 * @class EventParser
 * @brief Parses interactive I/O streams to mutate system state.
 *
 * Processes initial configuration data and ingests real-time simulation events
 * (ARR, TDN, XDN, FIN) to keep the global memory synchronized.
 */
class EventParser {
public:
  double current_time;                            // Current simulation timestamp.
  SystemParams params;                            // Global system parameters.
  std::map<int, TaskDurations> task_time_table;   // Table of execution times grouped by batch size.
  std::vector<TaskLog> task_history;              // Accumulated log of completed tasks.

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
    std::string token;
    std::cin >> token;

    if(token == "END" || std::cin.eof()) return false;

    current_time = std::stod(token);

    int event_count;
    std::cin >> event_count;

    for(int i = 0; i < event_count; i++) {
      std::string event_type;
      std::cin >> event_type;

      if(event_type == "ARR") {
        int r_id, length_in;
        std::cin >> r_id >> length_in;

        Request new_req(r_id, length_in);
        if (r_id >= static_cast<int>(state.all_request.size())) {
          state.all_request.resize(r_id + 1);
        }
        state.all_request[r_id] = std::move(new_req);
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
            for(int j = 0; j < log.m; j++) { int r_id; std::cin >> r_id; log.r_ids.push_back(r_id); }
            std::cin >> log.duration;
          } else if (step == "PROC") {
            std::cin >> log.remote >> log.m;
            for(int j = 0; j < log.m; j++) { int r_id; std::cin >> r_id; log.r_ids.push_back(r_id); }
            std::cin >> log.duration;
          } else if (step == "POST") {
            int minus_one; std::cin >> minus_one >> log.m;
            for(int j = 0; j < log.m; j++) { 
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

        for(int j = 0; j < m; j++) {
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
