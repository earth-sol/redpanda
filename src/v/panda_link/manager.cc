/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "panda_link/manager.h"

#include "panda_link/logger.h"

using namespace std::chrono_literals;

namespace panda_link {
manager::manager(::model::node_id self, std::unique_ptr<link_registry> registry)
  : _self(self)
  , _registry(std::move(registry)) {}

ss::future<> manager::start() {
    vlog(pllog.info, "Starting panda link manager");
    return ss::now();
}

ss::future<> manager::stop() {
    vlog(pllog.info, "Stopping panda link manager");
    return ss::now();
}

void manager::on_link_change(model::id_t id) {
    vlog(pllog.trace, "Panda link with id={} has changed", id);
}

void manager::on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader) {
    vlog(pllog.trace, "NTP={} leadership changed to {}", ntp, is_ntp_leader);
}
} // namespace panda_link
