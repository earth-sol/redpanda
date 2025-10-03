/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/topic_purger.h"

#include "base/vlog.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/logger.h"
#include "cluster/topic_table.h"

namespace cloud_topics::l1 {

topic_purger::topic_purger(
  metastore* metastore,
  cluster::topic_table* topics,
  remove_tombstone_fn_t remove_fn)
  : metastore_(metastore)
  , topics_(topics)
  , remove_tombstone_(std::move(remove_fn)) {}

ss::future<std::expected<void, topic_purger::error>>
topic_purger::purge_tombstoned_topics(ss::abort_source* as) {
    static constexpr auto max_topics_per_req = 100;
    static constexpr auto max_failed_metastore_attempts = 5;
    static constexpr auto max_concurrent_purges = 10;
    while (true) {
        const auto& tombstones = topics_->get_cloud_topic_tombstones();
        chunked_vector<model::topic_id> topics_to_remove;
        chunked_vector<cluster::nt_revision> ntrs_to_purge;
        for (const auto& [ntr, tombstone] : tombstones) {
            if (topics_to_remove.size() == max_topics_per_req) {
                break;
            }
            auto tid = tombstone.topic_id;
            topics_to_remove.emplace_back(tid);
            ntrs_to_purge.emplace_back(ntr);
        }
        if (topics_to_remove.empty()) {
            // No tombstones to remove! We're done!
            co_return std::expected<void, error>{};
        }

        size_t failed_attempts = 0;
        while (true) {
            // TODO: ensure all reconcilers for the given topics are stopped,
            // otherwise we may end up with orphaned topics in the metastore.
            auto remove_res = co_await metastore_->remove_topics(
              topics_to_remove);
            if (!remove_res.has_value()) {
                co_return std::unexpected(
                  error{fmt::format(
                    "Error removing topics from metastore: {}",
                    remove_res.error())});
            }
            const auto& resp = remove_res.value();
            if (resp.not_removed.empty()) {
                break;
            }
            ++failed_attempts;
            if (failed_attempts == max_failed_metastore_attempts) {
                co_return std::unexpected(
                  error{"Metastore requests failed too many times"});
            }
            if (as->abort_requested()) {
                co_return std::unexpected(error{"Shutting down topic purger"});
            }
            chunked_vector<model::topic_id> topics_to_retry;
            for (const auto& t : resp.not_removed) {
                topics_to_retry.push_back(t);
            }
            vlog(
              cd_log.debug,
              "Retrying removal of {} topics",
              topics_to_retry.size());
            topics_to_remove = std::move(topics_to_retry);
        }
        if (as->abort_requested()) {
            co_return std::unexpected(error{"Shutting down topic purger"});
        }
        // The metastore update has succeeded, go ahead and purge all the
        // topics we just removed.
        std::optional<topic_purger::error> first_error;
        auto purge_fut = co_await ss::coroutine::as_future(
          ss::max_concurrent_for_each(
            ntrs_to_purge,
            max_concurrent_purges,
            [this, &first_error](const cluster::nt_revision& ntr) {
                return remove_tombstone_(ntr).then(
                  [&first_error](
                    const topic_purger::remove_tombstone_ret_t& ret) {
                      if (!ret.has_value() && !first_error.has_value()) {
                          first_error = ret.error();
                      }
                  });
            }));
        if (purge_fut.failed()) {
            auto ex = purge_fut.get_exception();
            co_return std::unexpected(
              error{fmt::format("Exception purging tombstones: {}", ex)});
        }
        if (first_error.has_value()) {
            co_return std::unexpected(
              error{fmt::format("Error purging tombstones: {}", *first_error)});
        }
    }
}

} // namespace cloud_topics::l1
