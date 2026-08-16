/**
 * @file request.h
 * @brief Defines the Request entity and its lifecycle states within the scheduling system.
 * @author Authored by: Peeyush Maurya
 */

#pragma once

/**
 * @enum RequestState
 * @brief Represents the distinct phases of a Request's lifecycle.
 *
 * The lifecycle progresses from arrival at the edge, through prefill data transfers
 * to and from the cloud, and iterative decoding phases until completion.
 */
enum class RequestState {
  ARRIVED,              // Request has arrived, awaiting initial edge assignment.
  WAITING_PREFILL_UP,   // Prefill data is transferring from edge to cloud.
  READY_FOR_PREFILL,    // Request is at cloud, awaiting prefill processing.
  WAITING_PREFILL_DOWN, // Prefill output is transferring from cloud to edge.
  READY_FOR_P_POST,     // Request is at edge, awaiting prefill post-processing.
  READY_FOR_DECODE,     // Request is at edge, awaiting decode pre-processing.
  WAITING_DECODE_UP,    // Token data is transferring from edge to cloud.
  READY_FOR_D_PROC,     // Request is at cloud, awaiting decode processing.
  WAITING_DECODE_DOWN,  // Output token data is transferring from cloud to edge.
  READY_FOR_D_POST,     // Request is at edge, awaiting decode post-processing.
  FINISHED              // Request processing is fully completed.
};

/**
 * @struct Request
 * @brief Encapsulates all metadata and state information for a single task.
 *
 * Tracks the progression of a task, including its data lengths, assigned
 * compute nodes, and completed processing layers.
 */
struct Request {
  int id;               // Unique identifier for the request.
  int length_in;        // Input sequence length.
  int length_out;       // Number of output tokens generated.
  int assigned_cloud;   // Identifier of the cloud server assigned to this request.
  int layers_completed; // Number of neural network layers processed during prefill.
  RequestState state;   // Current phase in the request lifecycle.

  /**
   * @brief Constructs a new Request instance.
   * @param r_id The unique identifier.
   * @param r_length_in The initial input length.
   */
  Request(const int& r_id, const int& r_length_in) {
    id = r_id;
    length_in = r_length_in;
    length_out = 0;
    assigned_cloud = -1;
    layers_completed = 0;
    state = RequestState::ARRIVED;
  }
};
