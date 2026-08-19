/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for prefill layer processing.
 *        Small chunks under decode pressure enable the cloud to interleave
 *        P_PROC with D_PROC, keeping the decode pipeline responsive.
 *        Each chunk pays a fixed setup penalty S, so chunk size balances
 *        responsiveness (small) against setup overhead (large).
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

/**
 * @class Chunker
 * @brief Determines how many neural network layers to process in a single P_PROC call.
 *
 * Chunk=2 under decode pressure ensures the cloud re-checks for D_PROC work
 * every 2 layers. This keeps the decode pipeline flowing — critical for both
 * throughput (more decode cycles per second) and latency (lower TPOT).
 * Without this, a full-model P_PROC blocks the cloud for the entire prefill
 * duration, causing decode starvation.
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
            // Yield aggressively: re-check for decode work every 2 layers
            chunk_size = 2;
        } else {
            // No decode pressure: burst up to 16 layers to reduce setup overhead
            chunk_size = std::min(params.num_layers, 16);
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
