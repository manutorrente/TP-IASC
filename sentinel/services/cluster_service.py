from models.domain import Cluster


class ClusterService:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster

    def get_cluster(self) -> Cluster:
        return self.cluster
