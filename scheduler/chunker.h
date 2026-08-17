/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for prefill layer processing.
 *        Dynamically adjusts chunk sizes based on decode queue pressure
 *        to balance throughput against token latency constraints.
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

/**
 * @class Chunker
 * @brief Determines how many neural network layers to process in a single P_PROC call.
 *
 * When decode tasks are waiting, yields aggressively (chunk_size = 2) to minimize
 * token latency (SLO2). When no decodes are pending, bursts up to 16 layers to
 * reduce setup penalty overhead and maximize throughput.
 */
class Chunker {
public:
    /**
     * @brief Computes the ending layer index for the next prefill chunk.
     * @param ls Current starting layer index.
     * @param params System parameters containing total layer count.
     * @param decode_queue_size Number of decode tasks waiting on this cloud server.
     * @return The ending layer index for this chunk.
     */
    static int getNextChunkEnd(int ls, const SystemParams& params, size_t decode_queue_size = 0) {
        int chunk_size;
        
        if (decode_queue_size > 0) {
            // Yield to pending decodes: minimize blocking to protect SLO2
            chunk_size = 2;
        } else {
            // No decode pressure: burst to reduce setup penalty overhead
            chunk_size = std::min(params.num_layers, 16);
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
