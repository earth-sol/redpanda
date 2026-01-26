/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/level_zero_gc.h"

#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "model/fundamental.h"
#include "serde/protobuf/rpc.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shard_id.hh>

namespace {
ss::logger gclog("level_zero_gc_service");
} // namespace

namespace admin {

level_zero_gc_service_impl::level_zero_gc_service_impl(
  model::node_id self,
  admin::proxy::client pc,
  ss::sharded<cloud_topics::level_zero_gc>* gc,
  ss::sharded<cluster::members_table>* mt)
  : _self(self)
  , _proxy_client(std::move(pc))
  , _gc(gc)
  , _members_table(mt) {}

} // namespace admin
