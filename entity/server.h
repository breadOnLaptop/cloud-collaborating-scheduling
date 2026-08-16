#pragma once

// classification done as local or remote
enum ServerType {
  EDGE,
  CLOUD
};

// States for checking if server is occupied or not
enum ServerState {
  BUSY,
  FREE
};

struct Server {
  int id;             // range: [0, K - 1]
  ServerType type;    // either EDGE or CLOUD
  ServerState state;  // BUSY or FREE

  Server(
    const int& s_id,          // Server ID
    const ServerType& s_type  // Server Type: EDGE or CLOUD
  ) {
    id = s_id;
    type = s_type;
    state = ServerState::FREE;
  }

  void setBusy() {
    state = ServerState::BUSY;
  }

  void setFree() {
    state = ServerState::FREE;
  }
};
