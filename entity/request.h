#pragma once

enum class RequestState {
  ARRIVED,
  WAITING_PREFILL_UP,
  READY_FOR_PREFILL,
  WAITING_PREFILL_DOWN,
  READY_FOR_P_POST,
  READY_FOR_DECODE,
  WAITING_DECODE_UP,
  READY_FOR_D_PROC,
  WAITING_DECODE_DOWN,
  READY_FOR_D_POST,
  FINISHED
};

struct Request {
  int id;
  int length_in;
  int length_out;
  int assigned_cloud;
  int layers_completed;
  RequestState state;

  Request(const int& r_id, const int& r_length_in) {
    id = r_id;
    length_in = r_length_in;
    length_out = 0;
    assigned_cloud = -1;
    layers_completed = 0;
    state = RequestState::ARRIVED;
  }
};
