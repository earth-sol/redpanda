/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciliation_consumer.h"

#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::reconciler {

ss::future<std::optional<consumer_metadata>> build_from_reader(
  model::topic_id_partition tidp,
  model::record_batch_reader reader,
  l1::object_builder* builder) {
    auto gen = std::move(reader).slice_generator(model::no_timeout);
    co_await builder->start_partition(tidp);
    consumer_metadata metadata;
    while (auto batches = co_await gen()) {
        for (auto& batch : *batches) {
            if (metadata.base_offset == kafka::offset::min()) {
                metadata.base_offset = model::offset_cast(batch.base_offset());
            }
            metadata.last_timestamp = std::max(
              batch.header().max_timestamp, metadata.last_timestamp);
            metadata.last_offset = model::offset_cast(batch.last_offset());
            if (!metadata.terms.contains(batch.term())) {
                metadata.terms.insert(
                  std::make_pair(
                    batch.term(), model::offset_cast(batch.base_offset())));
            }
            ++metadata.batch_count;
            co_await builder->add_batch(std::move(batch));
        }
    }
    co_return metadata.batch_count == 0
      ? std::nullopt
      : std::make_optional(std::move(metadata));
}

} // namespace cloud_topics::reconciler
