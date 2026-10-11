#pragma once

#include <memory>
#include <optional>
#include <base/types.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>

/// Return most strict (by hard limit) system (non query-level, i.e. server/user/merges/...) memory limit
std::optional<UInt64> getMostStrictAvailableSystemMemory();

/// Returns the query memory threshold that makes an external operator spill: the smaller of the nonzero
/// `max_bytes_before_external` and the nonzero `max_bytes_ratio_before_external` fraction of the most strict
/// available system memory. The ratio has no effect without a system memory limit. Zero means no threshold.
/// The ratio must be at least 0 and less than 1; `ratio_setting_name` names it in the error, and `operator_name`
/// names the operator in the trace written to `log`.
size_t getMaxBytesBeforeExternalProcessing(
    size_t max_bytes_before_external,
    double max_bytes_ratio_before_external,
    const char * ratio_setting_name,
    const char * operator_name,
    const LoggerPtr & log);

std::optional<UInt64> getCurrentQueryHardLimit();

/// Return current query tracked memory usage
Int64 getCurrentQueryMemoryUsage();

/// The current query deliberately leaves memory to something that outlives it (e.g. the data of an in-memory table).
void setCurrentQueryMemoryDriftExpected();

/// The tracker of the user the current thread's memory is charged to, or `nullptr`.
MemoryTracker * getCurrentUserMemoryTracker();

/// Create a memory tracker under the current query memory tracker.
std::unique_ptr<MemoryTracker> tryCreateMemoryTrackerUnderCurrentQuery(VariableContext level = VariableContext::Thread);

/// Limit number of threads based on free memory.
/// If free memory (server limit minus tracked) is less than threads * min_free_per_thread,
/// returns the number of threads that fit, but at least 1.
/// Returns max_threads unchanged if min_free_per_thread is 0 or no server memory limit is set.
size_t getMaxThreadsForAvailableMemory(size_t max_threads, UInt64 min_free_per_thread);
