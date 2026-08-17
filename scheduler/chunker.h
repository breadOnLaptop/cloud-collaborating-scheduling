/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for heavy computational tasks.
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

/**
 * @class Chunker
 * @brief Calculates dynamic block sizes for neural network layer processing.
 *
 * Facilitates the division of prefill tasks into smaller, manageable chunks,
 * ensuring compute resources are regularly yielded for latency-sensitive operations.
 */
class Chunker {
public:
    /**
     * @brief Computes the ending layer index for the subsequent processing chunk.
     * @param ls The starting layer index.
     * @param params Global system parameters determining maximum layer depth.
     * @param decode_queue_size The current workload size of fast decode operations.
     * @return The calculated terminal layer index for the chunk.
     */
    static int getNextChunkEnd(int ls, const SystemParams& params, size_t decode_queue_size = 0) {
        int chunk_size = 8;
        
        if (decode_queue_size == 0) {
            chunk_size = 16;
        } else if (decode_queue_size > 16) {
            chunk_size = 2;
        } else if (decode_queue_size > 4) {
            chunk_size = 4;
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
