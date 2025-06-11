load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "panda_link",
    visibility = ["//src/v/panda_link:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "link.cc",
        "manager.cc",
    ],
    hdrs = [
        "link.h",
        "manager.h",
    ],
    implementation_deps = [
        ":logger",
    ],
    include_prefix = "panda_link",
    visibility = ["//src/v/panda_link:__subpackages__"],
    deps = [
        "//src/v/cluster",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/panda_link/model",
        "//src/v/ssx:work_queue",
        "@abseil-cpp//absl/container:flat_hash_map",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    include_prefix = "panda_link",
    visibility = ["//visibility:public"],
)
