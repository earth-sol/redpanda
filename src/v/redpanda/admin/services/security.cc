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

#include "redpanda/admin/services/security.h"

namespace admin {

seastar::future<proto::admin::resolve_oidc_identity_response>
security_service_impl::resolve_oidc_identity(
  serde::pb::rpc::context, proto::admin::resolve_oidc_identity_request) {
    throw serde::pb::rpc::unimplemented_exception(
      "resolve_oidc_identity is not implemented");
}

seastar::future<proto::admin::refresh_oidc_keys_response>
security_service_impl::refresh_oidc_keys(
  serde::pb::rpc::context, proto::admin::refresh_oidc_keys_request) {
    throw serde::pb::rpc::unimplemented_exception(
      "refresh_oidc_keys is not implemented");
}

seastar::future<proto::admin::revoke_oidc_sessions_response>
security_service_impl::revoke_oidc_sessions(
  serde::pb::rpc::context, proto::admin::revoke_oidc_sessions_request) {
    throw serde::pb::rpc::unimplemented_exception(
      "revoke_oidc_sessions is not implemented");
}

} // namespace admin
