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

#pragma once

#include "model/fundamental.h"
#include "panda_link/model/types.h"

namespace panda_link {

/// Indicates if the current node is the leader for a given NTP
using ntp_leader = ss::bool_class<struct is_ntp_leader_tag>;

/**
 * @brief Abstract class that provides accessors to panda link table
 *
 */
class link_registry {
public:
    link_registry() = default;
    link_registry(const link_registry&) = delete;
    link_registry(link_registry&&) = delete;
    link_registry& operator=(const link_registry&) = delete;
    link_registry& operator=(link_registry&&) = delete;
    virtual ~link_registry() = default;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
      find_link_by_id(model::id_t) const = 0;

    virtual std::optional<std::reference_wrapper<const model::metadata>>
    find_link_by_name(const model::name_t&) const = 0;
};

/**
 * @brief Class used to manage panda links
 *
 * This class will be notified of changes in Redpanda to create, modify, or
 * remove panda links and to handle leadership changes of partitions.
 */
class manager {
public:
    manager(::model::node_id self, std::unique_ptr<link_registry> registry);
    manager(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(const manager&) = delete;
    manager& operator=(manager&&) = delete;
    virtual ~manager() = default;

    ss::future<> start();
    ss::future<> stop();

    /// Used to notify that a panda link has been updated
    void on_link_change(model::id_t id);
    /// Used to notify manager in a change of NTP leadership
    void on_leadership_change(::model::ntp ntp, ntp_leader is_ntp_leader);

private:
    ::model::node_id _self;
    std::unique_ptr<link_registry> _registry;
    ntp_leader _is_controller_leader{ntp_leader::no};
};
} // namespace panda_link
