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

#include "utils/named_type.h"

#include <string_view>

namespace lsm {

// User keys are a named type wrapper to distinguish between internal keys and
// user provided keys (see internal::key for more information).
using user_key_view = named_type<std::string_view, struct user_key_view_tag>;

} // namespace lsm
