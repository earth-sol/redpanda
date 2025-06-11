load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "link_test",
    timeout = "short",
    srcs = [
        "link_test.cc",
    ],
    deps = [
        "//src/v/cluster",
        "//src/v/cluster/panda_link/tests:panda_link_test_lib",
        "//src/v/model",
        "//src/v/panda_link:impl",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
        "@seastar",
    ],
)
