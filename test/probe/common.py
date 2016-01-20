# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
from subprocess import Popen, PIPE
import sys
from time import sleep, time
from collections import defaultdict
import unittest
from nose import SkipTest
from datetime import timedelta
from time import sleep
import json

from six.moves.http_client import HTTPConnection

from swiftclient import get_auth, head_account
from swift.obj.diskfile import get_data_dir
from swift.common import internal_client
from swift.common.ring import Ring
from swift.common.utils import readconf, renamer, PivotTree, \
    pivot_to_pivot_container
from swift.common.manager import Manager
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY
from swift.common.wsgi import ConfigString

from test.probe import CHECK_SERVER_TIMEOUT, VALIDATE_RSYNC


ENABLED_POLICIES = [p for p in POLICIES if not p.is_deprecated]
POLICIES_BY_TYPE = defaultdict(list)
for p in POLICIES:
    POLICIES_BY_TYPE[p.policy_type].append(p)


def get_server_number(ipport, ipport2server):
    server_number = ipport2server[ipport]
    server, number = server_number[:-1], server_number[-1:]
    try:
        number = int(number)
    except ValueError:
        # probably the proxy
        return server_number, None
    return server, number


def start_server(ipport, ipport2server, pids, check=True):
    server, number = get_server_number(ipport, ipport2server)
    err = Manager([server]).start(number=number, wait=False)
    if err:
        raise Exception('unable to start %s' % (
            server if not number else '%s%s' % (server, number)))
    if check:
        return check_server(ipport, ipport2server, pids)
    return None


def check_server(ipport, ipport2server, pids, timeout=CHECK_SERVER_TIMEOUT):
    server = ipport2server[ipport]
    if server[:-1] in ('account', 'container', 'object'):
        if int(server[-1]) > 4:
            return None
        path = '/connect/1/2'
        if server[:-1] == 'container':
            path += '/3'
        elif server[:-1] == 'object':
            path += '/3/4'
        try_until = time() + timeout
        while True:
            try:
                conn = HTTPConnection(*ipport)
                conn.request('GET', path)
                resp = conn.getresponse()
                # 404 because it's a nonsense path (and mount_check is false)
                # 507 in case the test target is a VM using mount_check
                if resp.status not in (404, 507):
                    raise Exception(
                        'Unexpected status %s' % resp.status)
                break
            except Exception as err:
                if time() > try_until:
                    print err
                    print 'Giving up on %s:%s after %s seconds.' % (
                        server, ipport, timeout)
                    raise err
                sleep(0.1)
    else:
        try_until = time() + timeout
        while True:
            try:
                url, token = get_auth('http://%s:%d/auth/v1.0' % ipport,
                                      'test:tester', 'testing')
                account = url.split('/')[-1]
                head_account(url, token)
                return url, token, account
            except Exception as err:
                if time() > try_until:
                    print err
                    print 'Giving up on proxy:8080 after 30 seconds.'
                    raise err
                sleep(0.1)
    return None


def kill_server(ipport, ipport2server, pids):
    server, number = get_server_number(ipport, ipport2server)
    err = Manager([server]).kill(number=number)
    if err:
        raise Exception('unable to kill %s' % (server if not number else
                                               '%s%s' % (server, number)))
    try_until = time() + 30
    while True:
        try:
            conn = HTTPConnection(*ipport)
            conn.request('GET', '/')
            conn.getresponse()
        except Exception as err:
            break
        if time() > try_until:
            raise Exception(
                'Still answering on %s:%s after 30 seconds' % ipport)
        sleep(0.1)


def kill_nonprimary_server(primary_nodes, ipport2server, pids):
    primary_ipports = [(n['ip'], n['port']) for n in primary_nodes]
    for ipport, server in ipport2server.items():
        if ipport in primary_ipports:
            server_type = server[:-1]
            break
    else:
        raise Exception('Cannot figure out server type for %r' % primary_nodes)
    for ipport, server in list(ipport2server.items()):
        if server[:-1] == server_type and ipport not in primary_ipports:
            kill_server(ipport, ipport2server, pids)
            return ipport


def add_ring_devs_to_ipport2server(ring, server_type, ipport2server,
                                   servers_per_port=0):
    # We'll number the servers by order of unique occurrence of:
    #   IP, if servers_per_port > 0 OR there > 1 IP in ring
    #   ipport, otherwise
    unique_ip_count = len(set(dev['ip'] for dev in ring.devs if dev))
    things_to_number = {}
    number = 0
    for dev in filter(None, ring.devs):
        ip = dev['ip']
        ipport = (ip, dev['port'])
        unique_by = ip if servers_per_port or unique_ip_count > 1 else ipport
        if unique_by not in things_to_number:
            number += 1
            things_to_number[unique_by] = number
        ipport2server[ipport] = '%s%d' % (server_type,
                                          things_to_number[unique_by])


def store_config_paths(name, configs):
    for server_name in (name, '%s-replicator' % name):
        for server in Manager([server_name]):
            for i, conf in enumerate(server.conf_files(), 1):
                configs[server.server][i] = conf


def get_ring(ring_name, required_replicas, required_devices,
             server=None, force_validate=None, ipport2server=None,
             config_paths=None):
    if not server:
        server = ring_name
    ring = Ring('/etc/swift', ring_name=ring_name)
    if ipport2server is None:
        ipport2server = {}  # used internally, even if not passed in
    if config_paths is None:
        config_paths = defaultdict(dict)
    store_config_paths(server, config_paths)

    repl_name = '%s-replicator' % server
    repl_configs = {i: readconf(c, section_name=repl_name)
                    for i, c in config_paths[repl_name].items()}
    servers_per_port = any(int(c.get('servers_per_port', '0'))
                           for c in repl_configs.values())

    add_ring_devs_to_ipport2server(ring, server, ipport2server,
                                   servers_per_port=servers_per_port)
    if not VALIDATE_RSYNC and not force_validate:
        return ring
    # easy sanity checks
    if ring.replica_count != required_replicas:
        raise SkipTest('%s has %s replicas instead of %s' % (
            ring.serialized_path, ring.replica_count, required_replicas))
    if len(ring.devs) != required_devices:
        raise SkipTest('%s has %s devices instead of %s' % (
            ring.serialized_path, len(ring.devs), required_devices))
    for dev in ring.devs:
        # verify server is exposing mounted device
        ipport = (dev['ip'], dev['port'])
        _, server_number = get_server_number(ipport, ipport2server)
        conf = repl_configs[server_number]
        for device in os.listdir(conf['devices']):
            if device == dev['device']:
                dev_path = os.path.join(conf['devices'], device)
                full_path = os.path.realpath(dev_path)
                if not os.path.exists(full_path):
                    raise SkipTest(
                        'device %s in %s was not found (%s)' %
                        (device, conf['devices'], full_path))
                break
        else:
            raise SkipTest(
                "unable to find ring device %s under %s's devices (%s)" % (
                    dev['device'], server, conf['devices']))
        # verify server is exposing rsync device
        if conf.get('vm_test_mode', False):
            rsync_export = '%s%s' % (server, dev['replication_port'])
        else:
            rsync_export = server
        cmd = "rsync rsync://localhost/%s" % rsync_export
        p = Popen(cmd, shell=True, stdout=PIPE)
        stdout, _stderr = p.communicate()
        if p.returncode:
            raise SkipTest('unable to connect to rsync '
                           'export %s (%s)' % (rsync_export, cmd))
        for line in stdout.splitlines():
            if line.rsplit(None, 1)[-1] == dev['device']:
                break
        else:
            raise SkipTest("unable to find ring device %s under rsync's "
                           "exported devices for %s (%s)" %
                           (dev['device'], rsync_export, cmd))
    return ring


def get_policy(**kwargs):
    kwargs.setdefault('is_deprecated', False)
    # go through the policies and make sure they match the
    # requirements of kwargs
    for policy in POLICIES:
        # TODO: for EC, pop policy type here and check it first
        matches = True
        for key, value in kwargs.items():
            try:
                if getattr(policy, key) != value:
                    matches = False
            except AttributeError:
                matches = False
        if matches:
            return policy
    raise SkipTest('No policy matching %s' % kwargs)


class ProbeTest(unittest.TestCase):
    """
    Don't instantiate this directly, use a child class instead.
    """
    sharding_enabled = False

    def setUp(self):
        p = Popen("resetswift 2>&1", shell=True, stdout=PIPE)
        stdout, _stderr = p.communicate()
        print stdout
        Manager(['all']).stop()
        self.pids = {}
        try:
            self.ipport2server = {}
            self.configs = defaultdict(dict)
            self.account_ring = get_ring(
                'account',
                self.acct_cont_required_replicas,
                self.acct_cont_required_devices,
                ipport2server=self.ipport2server,
                config_paths=self.configs)
            self.container_ring = get_ring(
                'container',
                self.acct_cont_required_replicas,
                self.acct_cont_required_devices,
                ipport2server=self.ipport2server,
                config_paths=self.configs)
            self.policy = get_policy(**self.policy_requirements)
            self.object_ring = get_ring(
                self.policy.ring_name,
                self.obj_required_replicas,
                self.obj_required_devices,
                server='object',
                ipport2server=self.ipport2server,
                config_paths=self.configs)

            self.servers_per_port = any(
                int(readconf(c, section_name='object-replicator').get(
                    'servers_per_port', '0'))
                for c in self.configs['object-replicator'].values())

            Manager(['main']).start(wait=False)
            for ipport in self.ipport2server:
                check_server(ipport, self.ipport2server, self.pids)
            proxy_ipport = ('127.0.0.1', 8080)
            self.ipport2server[proxy_ipport] = 'proxy'
            self.url, self.token, self.account = check_server(
                proxy_ipport, self.ipport2server, self.pids)
            self.replicators = Manager(
                ['account-replicator', 'container-replicator',
                 'object-replicator'])
            self.updaters = Manager(['container-updater', 'object-updater'])
            self.container_sharder = Manager(['container-sharder'])
        except BaseException:
            try:
                raise
            finally:
                try:
                    Manager(['all']).kill()
                except Exception:
                    pass

    def tearDown(self):
        Manager(['all']).kill()

    def device_dir(self, server, node):
        server_type, config_number = get_server_number(
            (node['ip'], node['port']), self.ipport2server)
        repl_server = '%s-replicator' % server_type
        conf = readconf(self.configs[repl_server][config_number],
                        section_name=repl_server)
        return os.path.join(conf['devices'], node['device'])

    def storage_dir(self, server, node, part=None, policy=None):
        policy = policy or self.policy
        device_path = self.device_dir(server, node)
        path_parts = [device_path, get_data_dir(policy)]
        if part is not None:
            path_parts.append(str(part))
        return os.path.join(*path_parts)

    def config_number(self, node):
        _server_type, config_number = get_server_number(
            (node['ip'], node['port']), self.ipport2server)
        return config_number

    def is_local_to(self, node1, node2):
        """
        Return True if both ring devices are "local" to each other (on the same
        "server".
        """
        if self.servers_per_port:
            return node1['ip'] == node2['ip']

        # Without a disambiguating IP, for SAIOs, we have to assume ports
        # uniquely identify "servers".  SAIOs should be configured to *either*
        # have unique IPs per node (e.g. 127.0.0.1, 127.0.0.2, etc.) OR unique
        # ports per server (i.e. sdb1 & sdb5 would have same port numbers in
        # the 8-disk EC ring).
        return node1['port'] == node2['port']

    def get_to_final_state(self):
        # these .stop()s are probably not strictly necessary,
        # but may prevent race conditions
        self.replicators.stop()
        self.updaters.stop()

        self.replicators.once()
        self.updaters.once()
        self.replicators.once()

        self.container_sharder.stop()

    def kill_drive(self, device):
        if os.path.ismount(device):
            os.system('sudo umount %s' % device)
        else:
            renamer(device, device + "X")

    def revive_drive(self, device):
        disabled_name = device + "X"
        if os.path.isdir(disabled_name):
            renamer(device + "X", device)
        else:
            os.system('sudo mount %s' % device)

    def _get_pivot_tree(self, client, account, container):
        if not self.sharding_enabled or not getattr(self, 'shard_size', None):
            return

        path = client.make_path(account, container) + \
            '?nodes=pivot&format=json'
        try:
            resp = client.make_request('GET', path, {}, (2,))
            tree = PivotTree()
            pivots = json.loads(resp.body)
            if not pivots:
                return None
            for pivot in pivots:
                tree.add(pivot['name'])
        except Exception:
            tree = None

        return tree

    def shard_if_needed(self, account, container,
                        timeout=timedelta(minutes=2),
                        interval=timedelta(seconds=5)):
        """
        With the given information, shard a container only if sharding enabled
        :param account: the account containing the container
        :param container: the container to shard
        :param timeout: the total amount of time to allow for sharding
        :param interval: the amount of time to wait before attempting a shard

        requires shard_size to be set and sharding_enabled to be True
        """
        if not self.sharding_enabled or not getattr(self, 'shard_size', None):
            return

        config_string = '\n'.join(line.strip() for line in """
            [DEFAULT]
            swift_dir = /etc/swift

            [pipeline:main]
            pipeline = catch_errors cache proxy-server

            [app:proxy-server]
            use = egg:swift#proxy

            [filter:cache]
            use = egg:swift#memcache

            [filter:catch_errors]
            use = egg:swift#catch_errors
            """.split('\n'))

        swift = internal_client.InternalClient(ConfigString(config_string),
                                               'test', 1)

        path = swift.make_path(account, container)
        resp = swift.make_request('POST', path, {'X-Container-Sharding': 'On'},
                                  acceptable_statuses=(2,))
        resp = swift.make_request('GET', path, {}, acceptable_statuses=(2,))
        total_object_count = int(resp.headers['X-Container-Object-Count'])

        shard_containers = listings = None
        try_until = time() + timeout.total_seconds()
        while(True):
            self.container_sharder.start(once=True, wait=True)
            sleep(interval.total_seconds())

            pivot_tree = self._get_pivot_tree(swift, account, container)

            # may not be sharded yet
            if pivot_tree is None:
                continue

            # get all the sharded container accounts and container names
            shard_containers = []
            for leaf, weight in pivot_tree.leaves_iter():
                shard = pivot_to_pivot_container(account, container,
                                                 leaf.key, weight)
                shard_containers.append(shard)

            # get the container info of each sharded container
            listings = []
            for shard in shard_containers:
                path = swift.make_path(shard[0], shard[1])
                resp = swift.make_request('GET', path, {}, (2,))
                count = int(resp.headers['X-Container-Object-Count'])
                listings.append(count)

            # we are done if everything is done replicating etc
            if all(listing <= self.shard_size for listing in listings):
                if sum(listings) == total_object_count:
                    break

            if time() > try_until:
                state = dict(shard_size=self.shard_size,
                             leaf_containers=shard_containers,
                             objects_in_containers=listings,
                             total_objects=total_object_count)
                raise Exception('Timeout after %s seconds. state: %s' %
                                (timeout.total_seconds(), state))


class ReplProbeTest(ProbeTest):

    acct_cont_required_replicas = 3
    acct_cont_required_devices = 4
    obj_required_replicas = 3
    obj_required_devices = 4
    policy_requirements = {'policy_type': REPL_POLICY}


class ECProbeTest(ProbeTest):

    acct_cont_required_replicas = 3
    acct_cont_required_devices = 4
    obj_required_replicas = 6
    obj_required_devices = 8
    policy_requirements = {'policy_type': EC_POLICY}


if __name__ == "__main__":
    for server in ('account', 'container'):
        try:
            get_ring(server, 3, 4,
                     force_validate=True)
        except SkipTest as err:
            sys.exit('%s ERROR: %s' % (server, err))
        print '%s OK' % server
    for policy in POLICIES:
        try:
            get_ring(policy.ring_name, 3, 4,
                     server='object', force_validate=True)
        except SkipTest as err:
            sys.exit('object ERROR (%s): %s' % (policy.name, err))
        print 'object OK (%s)' % policy.name
