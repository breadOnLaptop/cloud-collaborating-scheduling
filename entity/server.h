/**
 * @file server.h
 * @brief Defines the Server entity, representing compute nodes in the network.
 * @author Authored by: opt1mal
 */

#pragma once

/**
 * @enum ServerType
 * @brief Categorizes the physical location and capability of a server.
 */
enum ServerType {
  EDGE,  // Local edge computer with low latency but limited capacity.
  CLOUD  // Remote cloud computer with high capacity but network latency overhead.
};

/**
 * @enum ServerState
 * @brief Indicates the current availability of a server.
 */
enum ServerState {
  BUSY,  // Server is currently executing a task.
  FREE   // Server is idle and available for task assignment.
};

/**
 * @struct Server
 * @brief Represents a compute node within the scheduling architecture.
 */
struct Server {
  int id;             // Unique identifier, typically bounded by system capacity.
  ServerType type;    // The classification of the server.
  ServerState state;  // The current execution status.

  /**
   * @brief Constructs a new Server instance.
   * @param s_id The unique identifier.
   * @param s_type The geographical and hardware classification.
   */
  Server(const int& s_id, const ServerType& s_type) {
    id = s_id;
    type = s_type;
    state = ServerState::FREE;
  }

  /**
   * @brief Marks the server as occupied.
   */
  void setBusy() {
    state = ServerState::BUSY;
  }

  /**
   * @brief Marks the server as idle.
   */
  void setFree() {
    state = ServerState::FREE;
  }
};
