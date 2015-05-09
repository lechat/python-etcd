import os
import pytest
import shutil
import logging
import time
import tempfile
import multiprocessing

import etcd
from . import helpers

log = logging.getLogger(__name__)


def run_process_helper(directory, program, clustered=False):
    return helpers.EtcdProcessHelper(
        directory,
        proc_name=program,
        port_range_start=6001,
        internal_port_range_start=8001,
        cluster=clustered)


def get_exe():
    PROGRAM = 'etcd'

    program_path = None

    for path in os.environ["PATH"].split(os.pathsep):
        path = path.strip('"')
        exe_file = os.path.join(path, PROGRAM)
        if os.path.isfile(exe_file) and os.access(exe_file, os.X_OK):
            program_path = exe_file
            break

    if not program_path:
        raise Exception('etcd not in path!!')

    return program_path


@pytest.fixture(scope='class')
def client(request):

    program = get_exe()
    directory = tempfile.mkdtemp(prefix='python-etcd')
    process_helper = run_process_helper(directory, program)
    process_helper.run(number=3)

    def tear_down():
        process_helper.stop()
        shutil.rmtree(directory)

    request.addfinalizer(tear_down)

    return etcd.Client(port=6001)


@pytest.fixture(scope='function')
def proc_helper(request):

    program = get_exe()
    directory = tempfile.mkdtemp(prefix='python-etcd')
    process_helper = run_process_helper(directory, program, clustered=True)
    process_helper.run(number=3)

    def tear_down():
        process_helper.stop()
        shutil.rmtree(directory)

    request.addfinalizer(tear_down)

    return process_helper


class TestEtcdBasicIntegration(object):

    def test_machines(self, client):
        """ INTEGRATION: retrieve machines """
        assert len(client.machines) == 1
        assert client.machines[0] == 'http://127.0.0.1:6001'

    def test_get_set_delete(self, client):
        """ INTEGRATION: set a new value """
        try:
            get_result = client.get('/test_set')
            assert False
        except etcd.EtcdKeyNotFound:
            pass

        assert '/test_set' not in client

        set_result = client.set('/test_set', 'test-key')
        assert 'set' == set_result.action.lower()
        assert '/test_set' == set_result.key
        assert 'test-key' == set_result.value

        assert '/test_set' in client

        get_result = client.get('/test_set')
        assert 'get' == get_result.action.lower()
        assert '/test_set' == get_result.key
        assert 'test-key' == get_result.value

        delete_result = client.delete('/test_set')
        assert 'delete' == delete_result.action.lower()
        assert '/test_set' == delete_result.key

        assert '/test_set' not in client

        try:
            get_result = client.get('/test_set')
            assert False
        except etcd.EtcdKeyNotFound:
            pass

    def test_update(self, client):
        """INTEGRATION: update a value"""
        client.set('/foo', 3)
        c = client.get('/foo')
        c.value = int(c.value) + 3
        client.update(c)
        newres = client.get('/foo')

        assert newres.value == u'6'
        with pytest.raises(ValueError):
            client.update(c)

    def test_retrieve_subkeys(self, client):
        """ INTEGRATION: retrieve multiple subkeys """
        client.write('/subtree/test_set', 'test-key1')
        client.write('/subtree/test_set1', 'test-key2')
        client.write('/subtree/test_set2', 'test-key3')
        get_result = client.read('/subtree', recursive=True)
        result = [subkey.value for subkey in get_result.leaves]

        assert ['test-key1', 'test-key2', 'test-key3'].sort() == result.sort()

    def test_directory_ttl_update(self, client):
        """ INTEGRATION: should be able to update a dir TTL """
        client.write('/dir', None, dir=True, ttl=30)
        res = client.write('/dir', None, dir=True, ttl=31, prevExist=True)

        assert res.ttl == 31

        res = client.get('/dir')
        res.ttl = 120
        new_res = client.update(res)

        assert new_res.ttl == 120

    def test_is_not_a_file(self, client):
        """ INTEGRATION: try to write  value to an existing directory """

        client.set('/directory/test-key', 'test-value')
        with pytest.raises(etcd.EtcdNotFile):
            client.set('/directory', 'test-value')

    def test_test_and_set(self, client):
        """ INTEGRATION: try test_and_set operation """

        client.set('/test-key', 'old-test-value')

        client.test_and_set('/test-key', 'test-value', 'old-test-value')

        with pytest.raises(ValueError):
            client.test_and_set('/test-key', 'new-value', 'old-test-value')

    def test_creating_already_existing_directory(self, client):
        """ INTEGRATION: creating an already existing directory without
        `prevExist=True` should fail """
        client.write('/mydir', None, dir=True)

        with pytest.raises(etcd.EtcdNotFile):
            client.write('/mydir', None, dir=True)
        with pytest.raises(etcd.EtcdAlreadyExist):
            client.write('/mydir', None, dir=True, prevExist=False)


class TestClusterFunctions(object):

    def test_reconnect(self, proc_helper):
        """ INTEGRATION: get key after the server we're connected fails. """
        client = etcd.Client(port=6001, allow_reconnect=True)
        client.set('/test_set', 'test-key1')
        get_result = client.get('/test_set')

        assert 'test-key1' == get_result.value

        proc_helper.kill_one(0)

        get_result = client.get('/test_set')
        assert 'test-key1' == get_result.value

    def test_reconnect_with_several_hosts_passed(self, proc_helper):
        """ INTEGRATION: receive several hosts at connection setup. """
        client = etcd.Client(
            host=(
                ('127.0.0.1', 6004),
                ('127.0.0.1', 6001)),
            allow_reconnect=True)
        client.set('/test_set', 'test-key1')
        get_result = client.get('/test_set')

        assert 'test-key1' == get_result.value

        proc_helper.kill_one(0)

        get_result = client.get('/test_set')
        assert 'test-key1' == get_result.value

    def test_reconnect_not_allowed(self, proc_helper):
        """ INTEGRATION: fail on server kill if not allow_reconnect """
        client = etcd.Client(port=6001, allow_reconnect=False)
        proc_helper.kill_one(0)
        with pytest.raises(etcd.EtcdException):
            client.get('/test_set')

    def test_reconnet_fails(self, proc_helper):
        """ INTEGRATION: fails to reconnect if no available machines """
        # Connect to instance 0
        client = etcd.Client(port=6001, allow_reconnect=True)
        client.set('/test_set', 'test-key1')

        get_result = client.get('/test_set')
        assert 'test-key1' == get_result.value
        proc_helper.kill_one(2)
        proc_helper.kill_one(1)
        proc_helper.kill_one(0)
        with pytest.raises(etcd.EtcdException):
            client.get('/test_set')

    def test_reconnect_to_failed_node(self, proc_helper):
        """ INTEGRATION: after a server failed and recovered we can connect."""
        # Connect to instance 0
        client = etcd.Client(port=6001, allow_reconnect=True)
        client.set('/test_set', 'test-key1')

        get_result = client.get('/test_set')
        assert 'test-key1' == get_result.value

        # kill 1 -> instances = (0, 2)
        proc_helper.kill_one(1)

        get_result = client.get('/test_set')
        assert 'test-key1' == get_result.value

        # # kill 0 -> Instances (2)
        # proc_helper.kill_one(0)

        # get_result = client.get('/test_set')
        # assertEquals('test-key1', get_result.value)

        # # Add 0 (failed server) -> Instances (0,2)
        # proc_helper.add_one(0)
        # # Instances (0, 2)

        # kill 2 -> Instances (0) (previously failed)
        proc_helper.kill_one(2)

        get_result = client.get('/test_set')
        assert 'test-key1' == get_result.value


class TestWatch(object):

    def test_watch(self, client):
        """ INTEGRATION: Receive a watch event from other process """

        client.set('/test-key', 'test-value')

        queue = multiprocessing.Queue()

        def change_value(key, newValue):
            c = etcd.Client(port=6001)
            c.set(key, newValue)

        def watch_value(key, queue):
            c = etcd.Client(port=6001)
            queue.put(c.watch(key).value)

        changer = multiprocessing.Process(
            target=change_value, args=('/test-key', 'new-test-value',))

        watcher = multiprocessing.Process(
            target=watch_value, args=('/test-key', queue))

        watcher.start()
        time.sleep(1)

        changer.start()

        value = queue.get(timeout=2)
        watcher.join(timeout=5)
        changer.join(timeout=5)

        assert value == 'new-test-value'

    def test_watch_indexed(self, client):
        """ INTEGRATION: Receive a watch event from other process, indexed """

        client.set('/test-key', 'test-value')
        set_result = client.set('/test-key', 'test-value0')
        original_index = int(set_result.modifiedIndex)
        client.set('/test-key', 'test-value1')
        client.set('/test-key', 'test-value2')

        queue = multiprocessing.Queue()

        def change_value(key, newValue):
            c = etcd.Client(port=6001)
            c.set(key, newValue)
            c.get(key)

        def watch_value(key, index, queue):
            c = etcd.Client(port=6001)
            for i in range(0, 3):
                queue.put(c.watch(key, index=index + i).value)

        proc = multiprocessing.Process(
            target=change_value, args=('/test-key', 'test-value3',))

        watcher = multiprocessing.Process(
            target=watch_value, args=('/test-key', original_index, queue))

        watcher.start()
        time.sleep(0.5)

        proc.start()

        for i in range(0, 3):
            value = queue.get()
            log.debug("index: %d: %s" % (i, value))
            assert 'test-value%d' % i == value

        watcher.join(timeout=5)
        proc.join(timeout=5)

    def test_watch_generator(self, client):
        """ INTEGRATION: Receive a watch event from other process (gen) """

        client.set('/test-key', 'test-value')

        queue = multiprocessing.Queue()

        def change_value(key):
            time.sleep(0.5)
            c = etcd.Client(port=6001)
            for i in range(0, 3):
                c.set(key, 'test-value%d' % i)
                c.get(key)

        def watch_value(key, queue):
            c = etcd.Client(port=6001)
            for i in range(0, 3):
                event = next(c.eternal_watch(key)).value
                queue.put(event)

        changer = multiprocessing.Process(
            target=change_value, args=('/test-key',))

        watcher = multiprocessing.Process(
            target=watch_value, args=('/test-key', queue))

        watcher.start()
        changer.start()

        values = ['test-value0', 'test-value1', 'test-value2']
        for i in range(0, 1):
            value = queue.get()
            log.debug("index: %d: %s" % (i, value))
            assert value in values

        watcher.join(timeout=5)
        changer.join(timeout=5)

    def test_watch_indexed_generator(self, client):
        """ INTEGRATION: Receive a watch event from other process, ixd, (2) """

        client.set('/test-key', 'test-value')
        set_result = client.set('/test-key', 'test-value0')
        original_index = int(set_result.modifiedIndex)
        client.set('/test-key', 'test-value1')
        client.set('/test-key', 'test-value2')

        queue = multiprocessing.Queue()

        def change_value(key, newValue):
            c = etcd.Client(port=6001)
            c.set(key, newValue)

        def watch_value(key, index, queue):
            c = etcd.Client(port=6001)
            iterevents = c.eternal_watch(key, index=index)
            for i in range(0, 3):
                queue.put(next(iterevents).value)

        proc = multiprocessing.Process(
            target=change_value, args=('/test-key', 'test-value3',))

        watcher = multiprocessing.Process(
            target=watch_value, args=('/test-key', original_index, queue))

        watcher.start()
        time.sleep(0.5)
        proc.start()

        for i in range(0, 3):
            value = queue.get()
            log.debug("index: %d: %s" % (i, value))
            assert 'test-value%d' % i == value

        watcher.join(timeout=5)
        proc.join(timeout=5)
