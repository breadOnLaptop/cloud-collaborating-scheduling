#pragma once

enum class RequestState {
  ARRIVED,              // Just Arrived, Waiting for edge
  WAITING_PREFILL_UP,   // Data is transferring to cloud
  READY_FOR_PREFILL,    // At cloud, waiting for P PROC
  WAITING_PREFILL_DOWN, // Data is transferring back to edge
  READY_FOR_P_POST,     // At edge, waiting for P POST
  READY_FOR_DECODE,     // At edge, waiting for D PRE
  WAITING_DECODE_UP,    // Small token data, transferring to cloud
  READY_FOR_D_PROC,     // At cloud, waiting for D PROC
  WAITING_DECODE_DOWN,  // Token data transferring back to edge
  READY_FOR_D_POST,     // At edge, waiting to finalize token
  FINISHED              // Request is completed
};

struct Request {
  int id;               // ARR provides
  int length_in;        // ARR provides
  int length_out;       // incremental based on tokens produced
  int assigned_cloud;   // locked in range of [0, K - 1] during P PRE
  int layers_completed; // tracks chunking progress during P PROC
  RequestState state;   // current state of request

  // ARR triggers at ARRIVED request state
  Request(
    const int& r_id,        // Request ID
    const int& r_length_in  // Request Length IN size
  ) {
    id = r_id, length_in = r_length_in;
    length_out = 0;
    assigned_cloud = -1;
    layers_completed = 0;
    state = RequestState::ARRIVED;
  }
};
