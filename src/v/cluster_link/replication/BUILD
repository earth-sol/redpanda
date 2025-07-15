load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "deps",
    hdrs = [
        "deps.h",
    ],
    include_prefix = "cluster_link/replication",
    visibility = ["__subpackages__"],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/raft",
    ],
)
