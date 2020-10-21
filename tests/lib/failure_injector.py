import docker
import os

class FailureInjector(object):

    def __init__(self, node=None):
        timeout = os.environ.get('DOCKER_CLIENT_TIMEOUT')
        if timeout is not None:
            timeout = int(timeout)
        self.docker_api = docker.from_env(timeout=timeout)

    def container_exec(self, node, command):
        docker_node = self.docker_api.containers.get(node)
        docker_node.exec_run(command, user='root')


class NoFailure(FailureInjector):

    def start(self):
        return

    def stop(self):
        return


class SingleNodePartition(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    def start(self):
        self.container_exec(self.node, "iptables -A INPUT -j DROP")
        self.container_exec(self.node, "iptables -A OUTPUT -j DROP")

    def stop(self):
        self.container_exec(self.node, "iptables -D INPUT -j DROP")
        self.container_exec(self.node, "iptables -D OUTPUT -j DROP")

class SingleNodePartitionReject(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    def start(self):
        self.container_exec(self.node, "iptables -A INPUT -j REJECT")
        self.container_exec(self.node, "iptables -A OUTPUT -j REJECT")

    def stop(self):
        self.container_exec(self.node, "iptables -D INPUT -j REJECT")
        self.container_exec(self.node, "iptables -D OUTPUT -j REJECT")


class EdgePartition(FailureInjector):

    def __init__(self, nodeA, nodeB):
        self.nodeA = nodeA
        self.nodeB = nodeB
        super().__init__()

    def __change(self, action):
        self.container_exec(self.nodeA,
                            "iptables {} INPUT -s {} -j DROP".format(
                                action, self.nodeB))
        self.container_exec(self.nodeA,
                            "iptables {} OUTPUT -d {} -j DROP".format(
                                action, self.nodeB))

    def start(self):
        self.__change('-A')

    def stop(self):
        self.__change('-D')


class RestartNode(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    # XXX: Is it really a good idea to call cli.stop inside method called start?
    def start(self):
        self.docker_api.containers.get(self.node).stop()

    def stop(self):
        self.docker_api.containers.get(self.node).start()


class FreezeNode(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    def start(self):
        self.docker_api.containers.get(self.node).pause()

    def stop(self):
        self.docker_api.containers.get(self.node).unpause()


class CrashRecoverNode(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    def start(self):
        self.docker_api.containers.get(self.node).kill()

    def stop(self):
        self.docker_api.containers.get(self.node).start()


class SkewTime(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

class StopNode(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    # XXX: Is it really a good idea to call cli.stop inside method called start?
    def start(self):
        self.docker_api.containers.get(self.node).stop()

    def stop(self):
        return


class StartNode(FailureInjector):

    def __init__(self, node):
        self.node = node
        super().__init__()

    # XXX: Is it really a good idea to call cli.stop inside method
    # called start?
    def start(self):
        return

    def stop(self):
        self.docker_api.containers.get(self.node).start()

ONE_NODE_FAILURES = [SingleNodePartition, SingleNodePartitionReject,
                     RestartNode, CrashRecoverNode, FreezeNode]
TWO_NODE_FAILURES = [EdgePartition]
