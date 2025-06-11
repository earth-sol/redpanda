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

#include "panda_link/link.h"

#include "panda_link/logger.h"

namespace panda_link {

link::link(model::metadata config)
  : _config(std::move(config)) {}

ss::future<> link::start() {
    vlog(pllog.info, "Starting panda link {} ({})", _config.name, _config.uuid);
    return ss::now();
}

ss::future<> link::stop() {
    vlog(pllog.info, "Stopping panda link {} ({})", _config.name, _config.uuid);
    return ss::now();
}

void link::update_config(model::metadata config) {
    vlog(
      pllog.debug,
      "Updating panda link {} ({}): {}",
      _config.name,
      _config.uuid,
      config);
    _config = std::move(config);
}

const model::metadata& link::config() const { return _config; }
} // namespace panda_link
