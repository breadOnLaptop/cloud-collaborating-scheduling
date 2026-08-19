/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for prefill layer processing.
 *        Provides two chunk strategies selectable by the scheduler:
 *        - Latency mode (chunk=8): reduces setup overhead for faster prefill
 *        - Throughput mode (chunk=2): maximizes decode responsiveness
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

/**
 * @class Chunker
 * @brief Determines how many neural network layers to process in a single P_PROC call.
 *
 * The chunk size is a key throughput-latency knob:
 *   - Larger chunks (8): fewer setup penalties S → faster prefill → lower TDR.
 *     Better for most tests including throughput-focused ones because faster
 *     prefill → more requests in decode → larger natural batches.
 *   - Smaller chunks (2): cloud re-checks for D_PROC every 2 layers → decode
 *     pipeline flows without long blocks. Needed only when the system is fully
 *     saturated and decode responsiveness is the critical bottleneck.
 */
class Chunker {
public:
    /**
     * @brief Computes the ending layer index for the next prefill chunk.
     * @param ls Current starting layer index.
     * @param params System parameters containing total layer count.
     * @param decode_queue_size Number of decode tasks waiting on this cloud server.
     * @param latency_mode True for chunk=8 strategy, false for chunk=2 strategy.
     * @return The ending layer index for this chunk.
     */
    static int getNextChunkEnd(int ls, const SystemParams& params, size_t decode_queue_size, bool latency_mode) {
        int chunk_size;
        
        if (decode_queue_size > 0) {
            // Yield to pending decodes: larger chunks for latency, smaller for throughput
            chunk_size = latency_mode ? std::min(params.num_layers, 8) : 2;
        } else {
            // No decode pressure: burst to reduce setup overhead
            chunk_size = latency_mode ? params.num_layers : std::min(params.num_layers, 16);
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
