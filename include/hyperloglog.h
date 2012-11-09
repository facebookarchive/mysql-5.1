#ifndef my_hyperloglog
#define my_hyperloglog

#include "my_global.h"
#include <inttypes.h>
#include <time.h>

C_MODE_START

/*
 * This is a custom built hyperloglog data structure intended for use
 * in counting number of distinct pages accessed in a table for the
 * last N minutes, where N is arbitrary. This is achieved by storing
 * the timestamp of the last time when a pho-value of j was achieved,
 * for each j from 1 to 32-log2(number of buckets)
 *
 * For the actual paper
 * P. Flajolet, E. Fusy, O. Gandouet, and F. Meunier.
 * Hyperloglog: The analysis of a near-optimal cardinality
 * estimation algorithm. In AofA 07: Proceedings of the 2007
 * International Conference on Analysis of Algorithms, June
 *
 * Look at Figure 3 for overview of algorithm implementation
 * Link as of 6/18/2012: algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 */

struct hyperloglog {
  /*
   * data[i * max_bit_position + j] stores the timestamp when a phi-value of 
   * j was obtained for bucket number i.
   */
  time_t* data;
  
  /*
   * data_size is the number of buckets, represented by 'm' in the original
   * paper. data_size = 2 ^ data_size_log2. In practice an integral value of
   * data_size_log2 is required for fast implementation.
   * max_bit_position is 32 - data_size_log2 + 1
   * Standard error is around (1 / sqrt(m)).
   * The data array above will be sized as (data_size)x(max_bit_position)
   * Useful value of data_size_log2 is in the range [4,16]
   */
  uint8_t data_size_log2;
  uint32_t data_size;
  uint8_t max_bit_position;
};

// Initialize the hyperloglog data structure with desired data_size_log2 value
void hyperloglog_init(struct hyperloglog* hll);

// Clears the data array, so that all counts are reset
void hyperloglog_reset(struct hyperloglog* hll);

// Insert the value hash into the data strucure, at time current_time
void hyperloglog_insert(
  struct hyperloglog* hll, 
  uint32_t hash, 
  time_t current_time);

// Get count of distinct elements inserted since since_time
uint64_t hyperloglog_query(struct hyperloglog* hll, time_t since_time);

// Destroy structure and free memory
void hyperloglog_destroy(struct hyperloglog* hll);

typedef struct hyperloglog hyperloglog_t;

C_MODE_END

#endif
