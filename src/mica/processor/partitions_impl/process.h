#pragma once
#ifndef MICA_PROCESSOR_PARTITIONS_IMPL_PROCESS_H_
#define MICA_PROCESSOR_PARTITIONS_IMPL_PROCESS_H_
#include <stdio.h>
#include <time.h>

void swap(uint16_t* xp, uint16_t* yp)
{
    uint16_t temp = *xp;
    *xp = *yp;
    *yp = temp;
}
uint32_t find_minimum(uint32_t a[], uint32_t n) {
  uint32_t c, index = 0;
  uint32_t min = a[0];
  for (c = 1; c < n; c++)
    if (a[c] < min){
      index = c;
      min = a[c];
  };

  return index;
}

namespace mica {
namespace processor {
template <class StaticConfig>
template <class RequestAccessor>
void Partitions<StaticConfig>::process(RequestAccessor& ra) {
  assert(::mica::util::lcore.lcore_id() != ::mica::util::LCore::kUnknown);
  uint16_t lcore_id = static_cast<uint16_t>(::mica::util::lcore.lcore_id());

  uint64_t stage_gap = load_stats_[lcore_id].stage_gap;
  // uint64_t pipeline_size = static_cast<uint64_t>(pipeline_size_);
  uint64_t pipeline_size = ::mica::util::next_power_of_two(3 * stage_gap);
  assert(pipeline_size > 0);
  uint64_t pipeline_size_mask = pipeline_size - 1;

  if (StaticConfig::kVerbose)
    printf("lcore %2" PRIu16 ": pipeline_size: %" PRIu64 "\n", lcore_id,
           pipeline_size);

  uint64_t start_t = ::mica::util::rdtsc();

  uint64_t last_gap_update_t = start_t;
  static const uint64_t gap_update_interval =
      0x10000;  // Must be a power of two.

  uint16_t partition_ids[pipeline_size];

  //uint16_t stats_lat[ra.num_threads][20000];
  uint16_t stats_lat_ops_per_iteration = 10000;
  uint64_t stats_cycles_per_iteration = 20; // 1ms
  uint16_t stats_lat[stats_lat_ops_per_iteration];
  //uint16_t stats_lat[stats_cycles_per_iteration];
  uint16_t stats_get_lat[stats_lat_ops_per_iteration];
  //uint16_t stats_get_lat[stats_cycles_per_iteration];
  uint64_t nset=0;
  uint64_t nget=0;
  uint64_t accumulated_set_lat=0;
  uint64_t accumulated_get_lat=0;
  uint32_t _99th_set_lat[stats_lat_ops_per_iteration/100]; // in total a 400B array
  uint32_t _99th_get_lat[stats_lat_ops_per_iteration/100]; // in total a 400B array

  // The maximum requests to handle at once.
  uint64_t count = static_cast<uint64_t>(static_cast<uint32_t>(-1));
  uint64_t count_plus_gap = count;
  count_plus_gap += 3 * stage_gap;

  // Note that we use uint64_t instead of int64_t even when we are dealing with
  // negative numbers.  The trick is that we can omit "index >= 0" checking by
  // using "index < count".
  for (uint64_t i_ = 0; i_ < count_plus_gap; i_++) {
    uint64_t index = i_;

    if (StaticConfig::kVerbose)
      printf("lcore %2" PRIu16 ": [0] i_ %" PRIu64 ", index %" PRIu64
             ", count %" PRIu64 ", count_plus_gap %" PRIu64 "\n",
             lcore_id, i_, index, count, count_plus_gap);

    if (count == static_cast<uint64_t>(static_cast<uint32_t>(-1))) {
      if (StaticConfig::kVerbose)
        printf("lcore %2" PRIu16 ": prepare index %" PRIu64 ", count %" PRIu64
               "\n",
               lcore_id, index, count);
      if (!ra.prepare(index)) {
        count = index;
        count_plus_gap = count;
        count_plus_gap += 3 * stage_gap;
      }
    }
    index -= stage_gap;

    if (StaticConfig::kVerbose)
      printf("lcore %2" PRIu16 ": [1] i_ %" PRIu64 ", index %" PRIu64
             ", count %" PRIu64 ", count_plus_gap %" PRIu64 "\n",
             lcore_id, i_, index, count, count_plus_gap);

    if (index < count) {
      auto key_hash = ra.get_key_hash(index);
      auto partition_id = get_partition_id(key_hash);
      partition_ids[index & pipeline_size_mask] = partition_id;
      if (StaticConfig::kPrefetchTable) {
        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16 ": prefetch_table index %" PRIu64
                 ", count %" PRIu64 "\n",
                 lcore_id, index, count);
        if (!StaticConfig::kSkipPrefetchingForRecentKeyHashes ||
            !is_recent_key_hash(lcore_id, key_hash)) {
          auto table = tables_[partition_id];
          table->prefetch_table(key_hash);
        }
      }
    }
    index -= stage_gap;

    if (StaticConfig::kVerbose)
      printf("lcore %2" PRIu16 ": [2] i_ %" PRIu64 ", index %" PRIu64
             ", count %" PRIu64 ", count_plus_gap %" PRIu64 "\n",
             lcore_id, i_, index, count, count_plus_gap);

    if (index < count) {
      if (StaticConfig::kPrefetchPool) {
        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16 ": prefetch_pool index %" PRIu64
                 ", count %" PRIu64 "\n",
                 lcore_id, index, count);
        auto key_hash = ra.get_key_hash(index);
        if (!StaticConfig::kSkipPrefetchingForRecentKeyHashes ||
            !is_recent_key_hash(lcore_id, key_hash)) {
          auto partition_id = partition_ids[index & pipeline_size_mask];
          auto table = tables_[partition_id];
          table->prefetch_pool(key_hash);
        }
      }
    }
    index -= stage_gap;

    if (StaticConfig::kVerbose)
      printf("lcore %2" PRIu16 ": [3] i_ %" PRIu64 ", index %" PRIu64
             ", count %" PRIu64 ", count_plus_gap %" PRIu64 "\n",
             lcore_id, i_, index, count, count_plus_gap);

    if (index < count) {
      if (StaticConfig::kVerbose)
        printf("lcore %2" PRIu16 ": process index %" PRIu64 ", count %" PRIu64
               "\n",
               lcore_id, index, count);
      auto operation = ra.get_operation(index);
      auto key_hash = ra.get_key_hash(index);
      auto partition_id = partition_ids[index & pipeline_size_mask];
      auto table = tables_[partition_id];
      auto owner_lcore_id = owner_lcore_ids_[partition_id];

      bool accept;
      switch (operation) {
        case Operation::kNoopRead:
        case Operation::kGet:
        case Operation::kTest:
          accept = concurrent_read_ || owner_lcore_id == lcore_id;
          break;
        case Operation::kReset:
        case Operation::kNoopWrite:
        case Operation::kAdd:
        case Operation::kSet:
        case Operation::kDelete:
        case Operation::kIncrement:
          accept = concurrent_write_ || owner_lcore_id == lcore_id;
          break;
        default:
          accept = true;
          break;
      }
      if (!accept) {
        ra.set_out_value_length(index, 0);
        ra.set_result(index, Result::kRejected);

        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16 ": retire index %" PRIu64 ", count %" PRIu64
                 "\n",
                 lcore_id, index, count);

        ra.retire(index);
      } else {
        Result result;

        load_stats_[lcore_id].request_count[partition_id]++;

        switch (operation) {
          case Operation::kReset:
            for (auto& table : tables_) table->reset();
            result = Result::kSuccess;
            ra.set_out_value_length(index, 0);
            break;
          case Operation::kNoopRead:
          case Operation::kNoopWrite:
            result = Result::kSuccess;
            ra.set_out_value_length(index, 0);
            break;
          case Operation::kAdd: {
            result = table->set(key_hash, ra.get_key(index),
                                ra.get_key_length(index), ra.get_value(index),
                                ra.get_value_length(index), false);
            ra.set_out_value_length(index, 0);
          } break;
          case Operation::kSet: {
            uint64_t per_iteration_start_t;
	    if (nset == 1) {
	      per_iteration_start_t = ::mica::util::rdtsc();
	      //printf("Core ID: %d", lcore_id, "Start a new iteration %u \n", per_iteration_start_t);
	    };
	    uint64_t per_op_start_t = ::mica::util::rdtsc();
	    result = table->set(key_hash, ra.get_key(index),
                                ra.get_key_length(index), ra.get_value(index),
                                ra.get_value_length(index), true);
            ra.set_out_value_length(index, 0);
	    uint64_t per_set_op_diff = ::mica::util::rdtsc() - per_op_start_t; 
	    //printf("%u \n", (uint16_t)per_op_diff);
	    //stats_lat[(uint16_t)lcore_id][(uint16_t)load_stats_[lcore_id].request_count[partition_id]] = per_op_diff;
	    stats_lat[nset] = per_set_op_diff;
	    accumulated_set_lat += per_set_op_diff;
	    uint32_t location; 
	    if (nset < stats_lat_ops_per_iteration/100) {// the array is not full
		_99th_set_lat[nset] = per_set_op_diff;
	    } else {
	        location = find_minimum(_99th_set_lat, (uint32_t)stats_lat_ops_per_iteration/100); // replace
                if (_99th_set_lat[location] <= per_set_op_diff) { // kick off the min
  		    _99th_set_lat[location] = per_set_op_diff;
		};
	    };
	    
	    //if (load_stats_[lcore_id].request_count[partition_id] % 20000 == 0) {
	    
	    // Record iteration based on teh number of ops 
	    if (nset % stats_lat_ops_per_iteration == stats_lat_ops_per_iteration - 1) {
	    // Record iteration based on the number of cycles
	    //if (::mica::util::rdtsc() - per_iteration_start_t == stats_cycles_per_iteration - 1) {
	      
          /* If check 99th percentile latency on the fly - write the file and analyze data offline*/ 
	      /*
	      FILE *fp;
              fp = fopen("latency_set.txt", "a");
              time_t t;
	      time(&t);
	      fprintf(fp, "\n Core ID: %d", lcore_id); //specific which core this process belongs to
	      fprintf(fp, "\nDate: %s", ctime(&t)); // Million Cycles
	      fprintf(fp, "Time: %u", ::mica::util::rdtsc() - per_iteration_start_t); // Million Cycles
              //fprintf(fp, "%u ", (unsigned int)lcore_id);
              //fprintf(fp, "%u ", (unsigned int)load_stats_[lcore_id].request_count[partition_id]);
	      //fwrite(stats_lat[(uint16_t)lcore_id], sizeof(uint16_t), sizeof(stats_lat[(uint16_t)lcore_id]), fp);
	      for (uint16_t m=0; m < stats_lat_ops_per_iteration; m++) {
	      // fit for both cycle-based or ops-based iteration
	      //for (uint16_t m=0; m < (int)(sizeof(stats_lat) / sizeof(stats_lat[0])); m++) {
		fprintf(fp, "%u \n", (uint16_t)stats_lat[m]);
	      };
	      fclose(fp);
	      */

	   /* If check the average latency on the flow*/
              //printf("SET %.2f %u %d (Avg, Time, CoreID)\n", (double)accumulated_set_lat / (double)nset, ::mica::util::rdtsc(), lcore_id);

	   /* If check the 99th latency on the flow*/
	      location = find_minimum(_99th_set_lat, (uint32_t)stats_lat_ops_per_iteration/100);
              printf("SET %u %u %d (99th, Time, CoreID)\n", _99th_set_lat[location], ::mica::util::rdtsc(), lcore_id);

	      //memset(stats_lat[(uint16_t)lcore_id], 0, sizeof stats_lat[(uint16_t)lcore_id]); 
	      memset(stats_lat, 0, sizeof stats_lat); 
	      memset(_99th_set_lat, 0, sizeof _99th_set_lat); 
	      nset = 0;
	      accumulated_set_lat = 0;
	    } else {
	      nset += 1;
	    };
            
	    //FILE *fp;
	    //fp = fopen("latency_set.txt", "a");
	    //fprintf(fp, "SET ");
	    //fprintf(fp, "%u ", (unsigned int)lcore_id);
	    //fprintf(fp, "%u ", (unsigned int)load_stats_[lcore_id].request_count[partition_id]);
	    //fprintf(fp, "%" PRId64 "\n", per_op_diff); 
            //fclose(fp);

	  } break;
          case Operation::kGet: {
            // TODO: Do not set allow_mutation to true for non-owner cores under
            // concurrent_write to reduce inter-core traffic.
            // bool allow_mutation =
            //     (concurrent_write_ || owner_lcore_ids_[partition_id] ==
            //     lcore_id);
            
	    uint64_t per_iteration_start_t;
            if (nget == 0) {
              per_iteration_start_t = ::mica::util::rdtsc();
            };				
	    uint64_t per_op_start_t = ::mica::util::rdtsc();
	    bool allow_mutation = owner_lcore_ids_[partition_id] == lcore_id;
            auto out_value = ra.get_out_value(index);
            auto out_value_length = ra.get_out_value_length(index);
            result = table->get(
                key_hash, ra.get_key(index), ra.get_key_length(index),
                out_value, out_value_length, &out_value_length, allow_mutation);
            if (result == Result::kSuccess || result == Result::kPartialValue)
              ra.set_out_value_length(index, out_value_length);
            else
              ra.set_out_value_length(index, 0);
	    uint64_t per_get_op_diff = ::mica::util::rdtsc() - per_op_start_t;

	    stats_get_lat[nget] = per_get_op_diff;
            accumulated_get_lat += per_get_op_diff;
	    uint32_t location;  
	    if (nget < stats_lat_ops_per_iteration/100) {// the array is not full
                _99th_get_lat[nset] = per_get_op_diff;
            } else {
                location = find_minimum(_99th_get_lat, (uint32_t)stats_lat_ops_per_iteration/100); // replace
                if (_99th_get_lat[location] <= per_get_op_diff) { // kick off the min
                    _99th_get_lat[location] = per_get_op_diff;
                };
            };

	    
	    // Record iteration based on teh number of ops
            if (nget % stats_lat_ops_per_iteration == stats_lat_ops_per_iteration - 1) {
            // Record iteration based on the number of cycles
            //if (::mica::util::rdtsc() - per_iteration_start_t == stats_cycles_per_iteration - 1) {
             
            /* If check 99th percentile latency on the fly - write the file and analyze data offline*/ 
	      /*	    
	      FILE *fp;
              fp = fopen("latency_get.txt", "a");
              time_t t;
              time(&t);

	      fprintf(fp, "\n Core ID: %d", lcore_id); //specific which core this process belongs to
              fprintf(fp, "\nDate: %s", ctime(&t)); // Million Cycles
              fprintf(fp, "Time: %u", ::mica::util::rdtsc() - per_iteration_start_t); // Million Cycles
              //for (uint16_t k=0; k < stats_lat_ops_per_iteration; k++) {
              // fit for both cycle-based or ops-based iteration
	      for (uint16_t k=0; k < (int)(sizeof(stats_get_lat) / sizeof(stats_get_lat[0])); k++) {
	        // sorting the array on the fly	
		//uint16_t min_idx = m;
		//for (uint16_t p=m+1; p < (int)(sizeof(stats_get_lat) / sizeof(stats_get_lat[0])); p++) {
		//  if (stats_get_lat[p] < stats_get_lat[min_idx])
	        //    min_idx = p;
		//};
		//swap(&stats_get_lat[min_idx], &stats_get_lat[m]);
	        fprintf(fp, "%u \n", (uint16_t)stats_get_lat[k]);
	      };
              fclose(fp);
	      */
	     
              /* If check the average latency on the flow*/
              //printf("GET %.2f %u %d (Avg, Time, CoreID)\n", (double)accumulated_get_lat / (double)nget, ::mica::util::rdtsc(), lcore_id);
              
              /* If check the 99th latency on the flow*/
              location = find_minimum(_99th_get_lat, (uint32_t)stats_lat_ops_per_iteration/100);
              printf("GET %u %u %d (99th, Time, CoreID)\n", _99th_get_lat[location], ::mica::util::rdtsc(), lcore_id);

              //memset(stats_lat[(uint16_t)lcore_id], 0, sizeof stats_lat[(uint16_t)lcore_id]); 
              memset(_99th_get_lat, 0, sizeof _99th_get_lat);
              memset(stats_get_lat, 0, sizeof stats_get_lat);
              nget = 0;
	      accumulated_get_lat = 0;
            } else {
              nget += 1;
            };


	    //printf("%" PRId64 "\n", per_op_diff);
            //FILE *fp;
            //fp = fopen("latency_get.txt", "a");
            //fprintf(fp, "GET ");
            //fprintf(fp, "%u ", (unsigned int)lcore_id);
            //fprintf(fp, "%u ", (unsigned int)load_stats_[lcore_id].request_count[partition_id]);
            //fprintf(fp, "%" PRId64 "\n", per_op_diff);
            //fclose(fp);
          } break;
          case Operation::kTest: {
            result = table->test(key_hash, ra.get_key(index),
                                 ra.get_key_length(index));
            ra.set_out_value_length(index, 0);
          } break;
          case Operation::kDelete: {
            result = table->del(key_hash, ra.get_key(index),
                                ra.get_key_length(index));
            ra.set_out_value_length(index, 0);
          } break;
          case Operation::kIncrement: {
            auto out_value = ra.get_out_value(index);
            auto in_value_length = ra.get_value_length(index);
            auto out_value_length = ra.get_out_value_length(index);
            if (in_value_length != sizeof(uint64_t) ||
                out_value_length < sizeof(uint64_t)) {
              result = Result::kError;
              ra.set_out_value_length(index, 0);
            } else {
              auto increment =
                  *reinterpret_cast<const uint64_t*>(ra.get_value(index));
              result = table->increment(key_hash, ra.get_key(index),
                                        ra.get_key_length(index), increment,
                                        reinterpret_cast<uint64_t*>(out_value));
              if (result == Result::kSuccess)
                ra.set_out_value_length(index, sizeof(uint64_t));
              else
                ra.set_out_value_length(index, 0);
            }
          } break;
          default:
            assert(false);
            result = Result::kError;
            ra.set_out_value_length(index, 0);
            break;
        }

        ra.set_result(index, result);

        if (StaticConfig::kVerbose)
          printf("lcore %2" PRIu16 ": retire index %" PRIu64 ", count %" PRIu64
                 "\n",
                 lcore_id, index, count);

        ra.retire(index);
      }

      if (StaticConfig::kSkipPrefetchingForRecentKeyHashes)
        update_recent_key_hash(lcore_id, key_hash);

      if (StaticConfig::kAutoStageGap) {
        if ((i_ & (gap_update_interval - 1)) == (gap_update_interval - 1) &&
            target_stage_gap_time_ != 0) {
          uint64_t gap_update_t = ::mica::util::rdtsc();
          uint64_t diff = gap_update_t - last_gap_update_t;
          last_gap_update_t = gap_update_t;

          uint64_t gap_time = stage_gap * diff / gap_update_interval;
          if (gap_time > 0)
            stage_gap = (stage_gap * target_stage_gap_time_ + (gap_time - 1)) /
                        gap_time;
          if (stage_gap < 1) stage_gap = 1;
          if (stage_gap > 16) stage_gap = 16;
        }
      }
    }

    if (StaticConfig::kVerbose)
      printf("lcore %2" PRIu16 ": [4] i_ %" PRIu64 ", index %" PRIu64
             ", count %" PRIu64 ", count_plus_gap %" PRIu64 "\n",
             lcore_id, i_, index, count, count_plus_gap);
  }

  uint64_t now = ::mica::util::rdtsc();

  if (StaticConfig::kAutoStageGap) {
    if (count_plus_gap < gap_update_interval - 1 &&
        target_stage_gap_time_ != 0) {
      uint64_t gap_update_t = now;
      uint64_t diff = gap_update_t - last_gap_update_t;
      last_gap_update_t = gap_update_t;

      uint64_t gap_time = stage_gap * diff / gap_update_interval;
      if (gap_time > 0)
        stage_gap =
            (stage_gap * target_stage_gap_time_ + (gap_time - 1)) / gap_time;
      if (stage_gap < 1) stage_gap = 1;
      if (stage_gap > 16) stage_gap = 16;
    }

    load_stats_[lcore_id].stage_gap = stage_gap;
  }

  uint64_t diff = now - start_t;
  load_stats_[lcore_id].processing_time += diff;

  if (StaticConfig::kVerbose) {
    if (count > 0) {
      printf("lcore %2hu: %" PRIu64 " clocks/req | gap time = %" PRIu64
             " clocks\n",
             lcore_id, diff / count, stage_gap * diff / count);
    }
  }

  apply_pending_owner_lcore_changes();
}

/*
template <class StaticConfig>
template <class RequestAccessor>
void Partitions<StaticConfig>::process(RequestAccessor& ra) {
  size_t count = ra.count();

  assert(::mica::util::lcore.lcore_id() != ::mica::util::LCore:kUnknown);
  uint16_t lcore_id = static_cast<uint16_t>(::mica::util::lcore.lcore_id());

  size_t stage0_index = 0;
  size_t stage1_index = 0;
  size_t stage2_index = 0;
  size_t stage3_index = 0;
  uint16_t stage1_index_wrapped = 0;
  uint16_t stage2_index_wrapped = 0;
  uint16_t stage3_index_wrapped = 0;

  uint16_t partition_ids[pipeline_size_];

  while (stage3_index < count) {
    if (StaticConfig::kVerbose)
      printf("%zu - %zu - %zu - %zu | %zu\n", stage0_index, stage1_index,
             stage2_index, stage3_index, count);
    if (stage0_index < count && stage0_index - stage1_index < stage_gap_ * 1) {
      ra.prefetch(stage0_index);
      stage0_index++;
    } else if (stage1_index < count &&
               stage1_index - stage2_index < stage_gap_ * 1) {
      auto operation = ra.get_operation(stage1_index);
      switch (operation) {
        case Operation::kReset:
          break;
        case Operation::kNoopRead:
        case Operation::kNoopWrite:
        case Operation::kAdd:
        case Operation::kGet:
        case Operation::kSet:
        case Operation::kTest:
        case Operation::kDelete:
        case Operation::kIncrement: {
          auto key_hash = ra.get_key_hash(stage1_index);
          uint16_t partition_id =
              get_partition_id(key_hash);
          partition_ids[stage1_index_wrapped] = partition_id;
          auto table = tables_[partition_id];
          table->prefetch_table(key_hash);
        } break;
        default:
          assert(false);
          break;
      }
      stage1_index++;
      stage1_index_wrapped++;
      if (stage1_index_wrapped == pipeline_size_) stage1_index_wrapped = 0;
    } else if (stage2_index < count &&
               stage2_index - stage3_index < stage_gap_ * 1) {
      switch (ra.get_operation(stage2_index)) {
        case Operation::kReset:
          break;
        case Operation::kNoopRead:
        case Operation::kNoopWrite:
        case Operation::kAdd:
        case Operation::kSet:
        case Operation::kGet:
        case Operation::kTest:
        case Operation::kDelete:
        case Operation::kIncrement: {
          auto key_hash = ra.get_key_hash(stage2_index);
          auto table = tables_[partition_ids[stage2_index_wrapped]];
          table->prefetch_pool(key_hash);
        } break;
        default:
          assert(false);
          break;
      }
      stage2_index++;
      stage2_index_wrapped++;
      if (stage2_index_wrapped == pipeline_size_) stage2_index_wrapped = 0;
    } else if (stage2_index > stage3_index) {
      switch (ra.get_operation(stage3_index)) {
        case Operation::kReset:
          for (auto& table : tables_) table->reset();
          break;
        case Operation::kNoopRead:
          *ra.get_out_value_length(stage3_index) = 0;
          ra.set_result(stage3_index, Result::kSuccess);
        case Operation::kNoopWrite:
          ra.set_result(stage3_index, Result::kSuccess);
          break;
        case Operation::kAdd: {
          auto key_hash = ra.get_key_hash(stage3_index);
          auto table = tables_[partition_ids[stage3_index_wrapped]];
          auto result = table->set(key_hash, ra.get_key(stage3_index),
                                   ra.get_key_length(stage3_index),
                                   ra.get_value(stage3_index),
                                   ra.get_value_length(stage3_index), false);
          ra.set_result(stage3_index, result);
        } break;
        case Operation::kSet: {
          auto key_hash = ra.get_key_hash(stage3_index);
          auto table = tables_[partition_ids[stage3_index_wrapped]];
          auto result = table->set(key_hash, ra.get_key(stage3_index),
                                   ra.get_key_length(stage3_index),
                                   ra.get_value(stage3_index),
                                   ra.get_value_length(stage3_index), true);
          ra.set_result(stage3_index, result);
        } break;
        case Operation::kGet: {
          auto key_hash = ra.get_key_hash(stage3_index);
          auto partition_id = partition_ids[stage3_index_wrapped];
          auto table = tables_[partition_id];
          bool allow_mutation =
              (concurrent_write_ ||
               owner_lcore_ids_[partition_id] ==
                   lcore_id);

          auto out_value = ra.get_out_value(stage3_index);
          auto in_value_length = ra.get_value_length(stage3_index);
          auto out_value_length = ra.get_out_value_length(stage3_index);
          auto result =
              table->get(key_hash, ra.get_key(stage3_index),
                         ra.get_key_length(stage3_index), out_value,
                         in_value_length, out_value_length, allow_mutation);
          ra.set_result(stage3_index, result);
        } break;
        case Operation::kTest: {
          auto key_hash = ra.get_key_hash(stage3_index);
          auto table = tables_[partition_ids[stage3_index_wrapped]];
          auto result = table->test(key_hash, ra.get_key(stage3_index),
                                    ra.get_key_length(stage3_index));
          ra.set_result(stage3_index, result);
        } break;
        case Operation::kDelete: {
          auto key_hash = ra.get_key_hash(stage3_index);
          auto table = tables_[partition_ids[stage3_index_wrapped]];
          auto result = table->del(key_hash, ra.get_key(stage3_index),
                                   ra.get_key_length(stage3_index));
          ra.set_result(stage3_index, result);
        } break;
        case Operation::kIncrement: {
          auto key_hash = ra.get_key_hash(stage3_index);
          auto table = tables_[partition_ids[stage3_index_wrapped]];
          auto out_value = ra.get_out_value(stage3_index);
          auto in_value_length = ra.get_value_length(stage3_index);
          if (in_value_length != sizeof(uint64_t)) {
            ra.set_result(stage3_index, Result::kError);
            break;
          }
          auto increment =
              *reinterpret_cast<const uint64_t*>(ra.get_value(stage3_index));
          auto result =
              table->increment(key_hash, ra.get_key(stage3_index),
                               ra.get_key_length(stage3_index), increment,
                               reinterpret_cast<uint64_t*>(out_value));
          ra.set_result(stage3_index, result);
        } break;
        default:
          assert(false);
          break;
      }
      stage3_index++;
      stage3_index_wrapped++;
      if (stage3_index_wrapped == pipeline_size_) stage3_index_wrapped = 0;
    }
  }
}
*/
}
}

#endif
