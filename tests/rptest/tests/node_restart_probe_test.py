# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from typing import Dict, Iterable, Sequence, Tuple
from contextlib import contextmanager
import time

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest

from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools


class Risks(dict):
    """a Dict[str, frozenset[str]] that makes sure keys are exactly these: """
    KEYS = frozenset(('rf1_offline', 'full_acks_produce_unavailable',
                      'unavailable', 'acks1_data_loss'))
    VALUE_RE = '^kafka/'  # ignore system topics

    @classmethod
    def build_value(cls, input: Iterable[str]):
        return frozenset(v for v in input if re.match(cls.VALUE_RE, v))

    def __init__(self, **kvargs):
        keys = kvargs.keys()
        assert keys == self.KEYS, f"{keys=}, {self.KEYS=}"
        dict.__init__(self, **{
            k: self.build_value(v)
            for k, v in kvargs.items()
        })


NO_RISKS = Risks(**{typ: set() for typ in Risks.KEYS})


class NodeRestartProbeTest(RedpandaTest):
    PRODUCE_BYTES = 300 * 1024 * 1024
    MSG_SIZE = 1024

    def __init__(self, test_context):
        super(NodeRestartProbeTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                'health_monitor_max_metadata_age': 100,  # ms
                'enable_leader_balancer': False
            })
        self.admin = Admin(self.redpanda)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def create_topics(self):
        self.topics = [
            TopicSpec(name='t1', partition_count=1, replication_factor=1),
            TopicSpec(name='t3', partition_count=2, replication_factor=3),
            TopicSpec(name='t5', partition_count=1, replication_factor=5),
        ]
        self.client().create_topic_with_assignment(self.topics[0].name, [[1]])
        self.client().create_topic_with_assignment(self.topics[1].name,
                                                   [[1, 2, 3], [3, 4, 5]])
        self.client().create_topic(self.topics[2])

    def get_node_risks(self, node, limit=None) -> Risks:
        reply = self.admin.get_broker_pre_restart_probe(node=node, limit=limit)
        self.redpanda.logger.debug(f"get_risks returned: {reply}")
        return Risks(**reply['risks'])

    def get_risks(self) -> Dict[int, Risks]:
        return {
            self.redpanda.node_id(node): self.get_node_risks(node)
            for node in self.redpanda.started_nodes()
        }

    def wait_pre_restart_probes(self,
                                expected_risks: Dict[int, Risks],
                                timeout_sec=30):
        """wait until it returns expected result, make sure it 
        does not return anything milder in the meanwhile"""
        def risks_are_as_expected():
            actual_risks = self.get_risks()
            self.redpanda.logger.debug(
                f"actual_risks={sorted(actual_risks.items())}, "
                f"expected_risks={sorted(expected_risks.items())}")
            return actual_risks == expected_risks

        wait_until(risks_are_as_expected,
                   timeout_sec=timeout_sec,
                   backoff_sec=0.1,
                   err_msg="Waiting for reported risks to match expected")

    def produce_to_all_partitions(self, acks):
        for topic in self.topics:
            self.redpanda.logger.debug(f"producing to {topic.name}")
            num_messages = int(topic.partition_count * self.PRODUCE_BYTES /
                               self.MSG_SIZE)
            self.kafka_tools.produce(topic.name, num_messages, self.MSG_SIZE,
                                     acks)
            self.redpanda.logger.debug(f"produced to {topic.name}")

    @contextmanager
    def with_append_entries_blocked(self, node,
                                    partitions: Sequence[Tuple[str, int]]):
        def block(block: bool):
            for topic, partition in partitions:
                self.redpanda.logger.info(
                    "toggle_block_partition_raft_op "
                    f"{topic=} {partition=} {node=} {block=}")
                self.admin.toggle_block_partition_raft_op(topic,
                                                          partition,
                                                          "append_entries",
                                                          block=block,
                                                          node=node)

        block(True)
        try:
            yield
        finally:
            block(False)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def node_restart_probe_test(self):
        nodes = {
            self.redpanda.node_id(node): node
            for node in self.redpanda.nodes
        }

        self.create_topics()
        t1 = self.topics[0].name
        t3 = self.topics[1].name
        t5 = self.topics[2].name
        t1p = f"kafka/{t1}/0"
        t3p0 = f"kafka/{t3}/0"
        t3p1 = f"kafka/{t3}/1"
        t5p = f"kafka/{t5}/0"

        # all nodes up
        inevitable_risks = {
            1:
            Risks(rf1_offline=[t1p],
                  full_acks_produce_unavailable=[],
                  unavailable=[],
                  acks1_data_loss=[]),
            2:
            NO_RISKS,
            3:
            NO_RISKS,
            4:
            NO_RISKS,
            5:
            NO_RISKS,
        }
        self.wait_pre_restart_probes(inevitable_risks)
        # limit 0 cuts off
        assert self.get_node_risks(nodes[1], limit=0) == NO_RISKS

        self.redpanda.stop_node(nodes[3])

        # node 3 down
        self.wait_pre_restart_probes({
            1:
            Risks(rf1_offline=[t1p],
                  full_acks_produce_unavailable=[],
                  unavailable=[t3p0],
                  acks1_data_loss=[]),
            2:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[],
                  unavailable=[t3p0],
                  acks1_data_loss=[]),
            4:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[],
                  unavailable=[t3p1],
                  acks1_data_loss=[]),
            5:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[],
                  unavailable=[t3p1],
                  acks1_data_loss=[]),
        })

        self.redpanda.start_node(nodes[3])
        # move t3_0, t3_1 and t5 leaders off node 3 which we will make lagged
        assert self.admin.transfer_leadership_to(namespace="kafka",
                                                 topic=t3,
                                                 partition=0,
                                                 target_id=2)
        assert self.admin.transfer_leadership_to(namespace="kafka",
                                                 topic=t3,
                                                 partition=1,
                                                 target_id=4)
        assert self.admin.transfer_leadership_to(namespace="kafka",
                                                 topic=t5,
                                                 partition=0,
                                                 target_id=2)
        self.redpanda.stop_node(nodes[5])

        # lag node 3
        with self.with_append_entries_error_injection(nodes[3], [(t3, 0),
                                                                 (t3, 1),
                                                                 (t5, 0)]):
            self.produce_to_all_partitions(acks=1)

        # node 3 lags, node 5 down
        self.wait_pre_restart_probes({
            1:
            Risks(rf1_offline=[t1p],
                  full_acks_produce_unavailable=[t3p0, t5p],
                  unavailable=[],
                  acks1_data_loss=[]),
            2:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[t3p0, t5p],
                  unavailable=[],
                  acks1_data_loss=[]),
            3:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[],
                  unavailable=[t3p1],
                  acks1_data_loss=[]),
            4:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[t5p],
                  unavailable=[t3p1],
                  acks1_data_loss=[t3p1]),
        })
        # good time to see how limits work
        assert len(
            self.get_node_risks(nodes[1],
                                limit=0)["full_acks_produce_unavailable"]) == 0
        assert len(
            self.get_node_risks(nodes[1],
                                limit=1)["full_acks_produce_unavailable"]) == 1
        assert len(
            self.get_node_risks(nodes[1],
                                limit=2)["full_acks_produce_unavailable"]) == 2
        assert len(
            self.get_node_risks(nodes[1],
                                limit=3)["full_acks_produce_unavailable"]) == 2

        # move t3_1 and t5 leaders off nodes 3 and 5 which we will make lagged
        assert self.admin.transfer_leadership_to(namespace="kafka",
                                                 topic=t3,
                                                 partition=1,
                                                 target_id=4)
        assert self.admin.transfer_leadership_to(namespace="kafka",
                                                 topic=t5,
                                                 partition=0,
                                                 target_id=2)
        # lag nodes 3 and 5
        self.redpanda.start_node(nodes[5])
        with self.with_append_entries_error_injection(nodes[3], [
            (t3, 0), (t3, 1), (t5, 0)
        ]), self.with_append_entries_error_injection(nodes[5], [(t3, 1),
                                                                (t5, 0)]):
            self.produce_to_all_partitions(acks=1)

        # all nodes up, but 3 and 5 lag
        self.wait_pre_restart_probes({
            1:
            Risks(rf1_offline=[t1p],
                  full_acks_produce_unavailable=[t3p0, t5p],
                  unavailable=[],
                  acks1_data_loss=[]),
            2:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[t3p0, t5p],
                  unavailable=[],
                  acks1_data_loss=[]),
            3:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[t3p1],
                  unavailable=[],
                  acks1_data_loss=[]),
            4:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[t5p],
                  unavailable=[],
                  acks1_data_loss=[t3p1]),
            5:
            Risks(rf1_offline=[],
                  full_acks_produce_unavailable=[t3p1],
                  unavailable=[],
                  acks1_data_loss=[]),
        })

        # when lag clears
        self.wait_pre_restart_probes(inevitable_risks, timeout_sec=240)
