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
 * Each P_PROC chunk pays a fixed setup penalty S. Fewer chunks = less overhead
 * but longer blocking of the cloud server. The chunk size adapts to decode
 * queue pressure on the specific cloud server:
 *   - No decodes waiting: process all remaining layers (zero extra S overhead)
 *   - Decodes waiting: chunk to 8 layers (balances throughput vs decode latency)
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
            // Yield to pending decodes with moderate chunks to balance S overhead vs latency
            chunk_size = std::min(params.num_layers, 8);
        } else {
            // No decode pressure: process all remaining layers to eliminate setup penalties
            chunk_size = params.num_layers;
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
