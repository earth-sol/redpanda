/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "metrics/host_metrics_watcher.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

namespace metrics {

TEST(DiskStatsParser, ParseDiskStatsGood) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590 3838657 2151727147 62187634 0 12832786 96772628 375506 274438 6540552016 27659340 958797 3600058
 259       1 nvme0n1p1 1182 1570 22668 1480 2 0 2 0 0 8279 9695 28 0 8307432 8215 0 0
 259       2 nvme0n1p2 1862 6 283442 252 48 23 544 92 0 260 358 28 0 1348816 13 0 0
 259       3 nvme0n1p3 18583337 9755 1060648508 2128597 10636142 2286095 1057802375 24987800 0 3052155 44045693 224239 165418 3637228912 16929295 0 0
 259       4 nvme0n1p4 1219720 3526 42457635 159682 6416068 266854 267304519 22854958 0 8635715 25491689 37477 28523 1199395944 2477048 0 0
 259       5 nvme0n1p5 6956407 6051 325499183 1035572 8998278 1285685 826619707 14344527 0 1520717 23624869 113734 80497 1694270912 8244768 0 0
 253       0 dm-0 1217871 0 42450873 168534 6682917 0 267304519 81228952 0 9277885 127118565 66000 0 1199395944 45721079 0 0
 252       0 zram0 969732 0 7766128 3504 2911882 0 23295056 17249 0 70594 20753 0 0 0 0 0 0
 253       1 dm-1 18587723 0 1060641794 2394466 12922232 0 1057802375 1126028723 0 3504151 1562828774 389657 0 3637228912 434405585 0 10
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 9);
    ASSERT_EQ(disk_stats["nvme0n1"].size(), 17);
    ASSERT_EQ(disk_stats["nvme0n1"][0], 26762678);
    ASSERT_EQ(disk_stats["nvme0n1"][16], 3600058);

    ASSERT_EQ(disk_stats["dm-1"].size(), 17);
    ASSERT_EQ(disk_stats["dm-1"][0], 18587723);
    ASSERT_EQ(disk_stats["dm-1"][16], 10);
}

TEST(DiskStatsParser, ParseDiskStatsOldKernel) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590 3838657 2151727147 62187634 0 12832786 96772628
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 1);
    ASSERT_EQ(disk_stats["nvme0n1"].size(), 17);
    ASSERT_EQ(disk_stats["nvme0n1"][0], 26762678);
    ASSERT_EQ(disk_stats["nvme0n1"][16], 0);
}

TEST(DiskStatsParser, ParseDiskStatsTooOld) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 0);
}

TEST(DiskStatsParser, ParseDiskStatsGarbage) {
    std::string_view diskstats = R""""(
 259       0 nvme0n1 26762678 20908 1428918348 3325593 26050590 asdf 2151727147 62187634 0 12832786 96772628 375506 274438 6540552016 27659340 958797 3600058
)"""";

    host_metrics_watcher::diskstats_map disk_stats;
    EXPECT_THROW(
      host_metrics_watcher::parse_diskstats(diskstats, disk_stats),
      std::invalid_argument);
}

TEST(DiskStatsParser, ParseDiskStatsEmpty) {
    std::string_view diskstats = "";

    host_metrics_watcher::diskstats_map disk_stats;
    host_metrics_watcher::parse_diskstats(diskstats, disk_stats);

    ASSERT_EQ(disk_stats.size(), 0);
}

} // namespace metrics
