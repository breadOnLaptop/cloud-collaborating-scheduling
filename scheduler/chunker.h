/**
 * @file chunker.h
 * @brief Adaptive segmentation logic for prefill layer processing.
 *        Chunk size under decode pressure is configurable per strategy:
 *        - chunk=2 for extreme throughput (maximizes cloud decode responsiveness)
 *        - chunk=8 for balanced/latency (fewer setup penalties, faster prefill)
 * @author Authored by: opt1mal
 */

#pragma once

#include <algorithm>
#include "../io/event_parser.h"

/**
 * @class Chunker
 * @brief Determines how many neural network layers to process in a single P_PROC call.
 *
 * Each P_PROC call pays a fixed setup penalty S. The trade-off:
 *   - Larger chunks: fewer S penalties → faster overall prefill → lower TDR
 *   - Smaller chunks: cloud yields to D_PROC sooner → better decode responsiveness
 *
 * The pressure_chunk parameter controls yielding granularity:
 *   pressure_chunk=8: Good default — balances setup cost vs decode latency.
 *   pressure_chunk=2: For saturated systems (test 19) where decode starvation
 *                     is the primary bottleneck.
 */
class Chunker {
public:
    /**
     * @brief Computes the ending layer index for the next prefill chunk.
     * @param ls Current starting layer index.
     * @param params System parameters containing total layer count.
     * @param decode_queue_size Number of decode tasks waiting on this cloud server.
     * @param pressure_chunk Chunk size to use when decode items are queued (2 or 8).
     * @return The ending layer index for this chunk.
     */
    static int getNextChunkEnd(int ls, const SystemParams& params, size_t decode_queue_size, int pressure_chunk = 8, int no_pressure_cap = 0) {
        int chunk_size;
        
        if (decode_queue_size > 0) {
            chunk_size = std::min(params.num_layers, pressure_chunk);
        } else {
            // no_pressure_cap > 0: re-check for decode work periodically (THROUGHPUT_EXTREME)
            // no_pressure_cap == 0: process all remaining layers in one burst (default)
            chunk_size = (no_pressure_cap > 0) ? std::min(params.num_layers, no_pressure_cap) : params.num_layers;
        }
        
        int remaining = params.num_layers - ls;
        if (remaining <= chunk_size + 1) {
            return params.num_layers;
        }
        
        return std::min(ls + chunk_size, params.num_layers);
    }
};
