load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "writer_test",
    timeout = "short",
    srcs = ["writer_test.cc"],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:writer",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/cleanup",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
