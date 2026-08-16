#pragma once

enum class ServerType {
  EDGE,
  CLOUD
};

enum class ServerState {
  BUSY,
  FREE
};

struct Server {
  int id;
  ServerType type;
  ServerState state;

  Server(const int& s_id, const ServerType& s_type) {
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
