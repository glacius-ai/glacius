from enum import Enum
from typing import Any, Dict, Optional


class Runtime(Enum):
    EMR = "EMR"
    DATABRICKS = "Databricks"
    GCP = "GCP"


class JobStatus(Enum):
    REQUESTED = "REQUESTED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobType(Enum):
    MATERIALIZATION = "MATERIALIZATION"
    OFFLINE_FEATURES_COMPUTATION = "OFFLINE_FEATURES_COMPUTATION"
    FEATURE_QUALITY_CHECK = "FEATURE_QUALITY_CHECK"


class ComputeTier(Enum):
    XS = "XS"
    S = "S"
    M = "M"
    L = "L"
    XL = "XL"


class Job:
    """Represents a job with its specifications."""

    def __init__(
        self,
        *,
        namespace: str,
        namespace_version: str,
        provider_region: str,
        runtime: Runtime,
        job_status: JobStatus,
        inputs: Dict[str, Any],
        job_type: JobType,
        num_workers: int = 4,
        outputs: Optional[str] = None,
        exception_info: Optional[str] = None,
        job_id: Optional[int] = None,  # only exists for triggered jobs
        created_at: Optional[str] = None,  # datetime type for created_at
        log_destination: Optional[str] = None,
        cluster_id: Optional[str] = None,
        step_id: Optional[str] = None,
        compute_tier: Optional[ComputeTier] = None,
        duration: Optional[int] = None,
        workspace: Optional[str] = None,
    ):
        self._namespace = namespace
        self._namespace_version = namespace_version
        self._runtime = runtime
        self._job_status = job_status
        self._provider_region = provider_region
        self._inputs = inputs
        self._outputs = outputs
        self._exception_info = exception_info
        self._job_type = job_type
        self._job_id = job_id
        self._created_at = created_at
        self._log_destination = log_destination
        self._cluster_id = cluster_id
        self._step_id = step_id
        self._compute_tier = compute_tier
        self._num_workers = num_workers
        self._duration = duration
        self._workspace = workspace

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def namespace_version(self) -> str:
        return self._namespace_version

    @property
    def runtime(self) -> Runtime:
        return self._runtime

    @property
    def job_status(self) -> JobStatus:
        return self._job_status

    @property
    def inputs(self) -> Dict[str, Any]:
        return self._inputs

    @property
    def provider_region(self) -> str:
        return self._provider_region

    @property
    def outputs(self) -> Optional[str]:
        return self._outputs

    @property
    def exception_info(self) -> Optional[str]:
        return self._exception_info

    @property
    def job_type(self) -> JobType:
        return self._job_type

    @property
    def job_id(self) -> int:
        return self._job_id

    @property
    def created_at(self) -> str:
        return self._created_at

    @property
    def log_destination(self) -> str:
        return self._log_destination

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    @cluster_id.setter
    def cluster_id(self, value: str):
        self._cluster_id = value

    @property
    def step_id(self) -> str:
        return self._step_id

    @step_id.setter
    def step_id(self, value: str):
        self._step_id = value

    @property
    def compute_tier(self) -> str:
        return self._compute_tier

    @compute_tier.setter
    def compute_tier(self, value: str):
        self._compute_tier = value

    @log_destination.setter
    def log_destination(self, value: str):
        self._log_destination = value

    @property
    def num_workers(self):
        return self._num_workers

    @property
    def duration(self) -> int:
        return self._duration

    @duration.setter
    def duration(self, value: int):
        self._duration = value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": "TESTING",
            "namespace": self.namespace,
            "namespace_version": self.namespace_version,
            "runtime": self.runtime.value,
            "provider_region": self.provider_region,
            "job_status": self.job_status.value,
            "job_id": self.job_id,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "exception_info": self.exception_info,
            "job_type": self.job_type.value,
            "created_at": self.created_at,
            "log_destination": self.log_destination,
            "cluster_id": self.cluster_id,
            "step_id": self.step_id,
            "compute_tier": self.compute_tier.value if self.compute_tier else None,
            "num_workers": self.num_workers,
            "duration": self.duration,
            "workspace": self.workspace,
        }

    @classmethod
    def from_dict(cls, data_dict: Dict[str, Any]) -> "Job":
        return cls(
            namespace=data_dict["namespace"],
            namespace_version=data_dict["namespace_version"],
            runtime=Runtime(data_dict["runtime"]),
            job_status=JobStatus(data_dict["job_status"]),
            provider_region=data_dict["provider_region"],
            inputs=data_dict["inputs"],
            job_id=data_dict["job_id"] if "job_id" in data_dict else None,
            outputs=data_dict.get("outputs"),
            exception_info=data_dict.get("exception_info"),
            job_type=JobType(data_dict["job_type"]),
            created_at=data_dict["created_at"] if "created_at" in data_dict else None,
            log_destination=data_dict["log_destination"]
            if "log_destination" in data_dict
            else None,
            cluster_id=data_dict["cluster_id"] if "cluster_id" in data_dict else None,
            step_id=data_dict["step_id"] if "step_id" in data_dict else None,
            compute_tier=ComputeTier(data_dict["compute_tier"])
            if "compute_tier" in data_dict
            else None,
            num_workers=data_dict["num_workers"],
            duration=data_dict["duration"] if "duration" in data_dict else None,
            workspace=data_dict["workspace"] if "workspace" in data_dict else None,
        )
