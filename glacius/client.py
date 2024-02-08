import logging
from typing import List, Optional

import requests
from requests.exceptions import HTTPError

from glacius import Feature, FeatureBundle, Job
from glacius.data_sources.source import DataSource
from glacius.derived_feature import DerivedFeature
from glacius.job import JobStatus, JobType, Runtime, ComputeTier

logger = logging.getLogger(__name__)

API_URL = "https://app.glacius.ai"


class Client:    

    def __init__(
        self,
        api_key: str,
        namespace: str,
    ):
        """Generates a glacius client instance

        Args:
            api_key (str): API key 
            namespace (str): The namespace
        """
        self._api_key = api_key
        self._namespace = namespace
        self._api_key = api_key
        self._workspace = self._get_workspace_from_key(api_key)

    @property
    def api_key(self):
        return self._api_key

    @property
    def namespace(self):
        return self._namespace

    @property
    def workspace(self):
        return self._workspace

    @property
    def stub(self):
        return self._stub


    def _get_workspace_from_key(self, api_key: str):
        api_endpoint = f"{API_URL}/workspace/api_key/{api_key}"
        try:
            response = requests.get(api_endpoint)            
            if response.status_code == 200:
                return response.json()
            else:                
                logging.error(
                    f"Failed to get workspace: {response.status_code} - {response.text}"
                )                
                raise Exception("Unauthorized access or invalid API key")
        except requests.RequestException as e:            
            logging.error(f"Error during requests call: {e}")
            raise

    def get_online_features(self, feature_names: List[str], entity_ids: List[str]):        
        try:
            headers = {"X-API-Key": self.api_key}
            online_features_api = f"{API_URL}/online-store"
            payload = {
                "namespace": self.namespace,  
                "feature_names": feature_names,
                "entity_ids": entity_ids,
            }

            response = requests.post(online_features_api, json=payload, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.exception(f"Failed to get online features: {response.status_code}")
            
                return None
        except Exception as e:
            pass

    def get_offline_features(
        self,
        labels_datasource: DataSource,
        output_path: str,
        compute_tier: str = "M",
        namespace_version: str = "latest",
        num_workers: int = 8,
        feature_names: Optional[List[str]] = None,
        feature_bundles: Optional[List[FeatureBundle]] = None,
        derived_feature: Optional[List[Feature]] = None,
    ):
        try:
            if feature_names and feature_bundles:
                raise ValueError(
                    "Specify feature names if you'd like to pull definitions from the registry. For ad hoc runs specify feature bundles and derived features"
                )

            namespace = self.namespace
            runtime = "EMR"

            if feature_names:
                request_body = {"feature_names": feature_names}

                headers = {"X-API-Key": self.api_key}

                features_api_url = (
                    f"{API_URL}/namespace/{namespace}/{namespace_version}/filter-server"
                )
                response = requests.post(
                    features_api_url, json=request_body, headers=headers
                )

                response.raise_for_status()
                response_deser = response.json()

                inputs = {
                    "labels_datasource": labels_datasource.to_dict(),
                    "feature_bundles": [
                        bundle_dict
                        for bundle_dict in response_deser.get("feature_bundles")
                    ],
                    "derived_features": [
                        feature_dict
                        for feature_dict in response_deser.get("derived_features")
                    ],
                    "output_path": output_path,
                }
            else:
                inputs = {
                    "labels_datasource": labels_datasource.to_dict(),
                    "feature_bundles": [fb.to_dict() for fb in feature_bundles],
                    "derived_feature": [df.to_dict() for df in derived_feature],
                    "output_path": output_path,
                }

            job = Job(
                namespace=namespace,                
                provider_region="us-east-1",
                namespace_version=namespace_version,
                runtime=Runtime(runtime),
                job_status=JobStatus.PENDING,
                inputs=inputs,
                job_type=JobType.OFFLINE_FEATURES_COMPUTATION,
                compute_tier=ComputeTier[compute_tier],
                num_workers=num_workers,
                workspace=self.workspace,
            )

  
            job_data = job.to_dict()
              
            api_endpoint = (
                f"{API_URL}/jobs/{self.workspace}/{namespace}/{namespace_version}"
            )

            response = requests.post(api_endpoint, json=job_data, headers=headers)
            job_dict = response.json().get("job")
            response.raise_for_status()

            return Job.from_dict(job_dict)
        except HTTPError as e:
            if e.response is not None:
                raise Exception(f"{e.response.json().get('detail')}")

    def register(
        self,
        feature_bundles: List[FeatureBundle],
        commit_msg: str,
        derived_features: Optional[List[DerivedFeature]] = None,
    ):
        # The URL where your FastAPI server is running
        try:
            headers = {"X-API-Key": self.api_key}
            api_endpoint = f"{API_URL}/namespace/{self.workspace}/{self.namespace}/register_features"

            # Convert your objects to their JSON representations
            feature_bundles_json = [fb.to_json() for fb in feature_bundles]
            derived_features_json = (
                [df.to_json() for df in derived_features] if derived_features else None
            )

            payload = {
                "feature_bundles": feature_bundles_json,
                "derived_features": derived_features_json,
                "commit_msg": commit_msg,
            }

            response = requests.post(api_endpoint, json=payload, headers=headers)

            # Raise an error if the request was unsuccessful
            response.raise_for_status()

            return (
                response.json()
            )  # Return the server's response, which will contain any potential messages or errors

        except HTTPError as e:
            if e.response is not None:
                raise Exception(f"{e.response.json().get('detail')}")


    def materialize_features(
        self,
        feature_names: List[str],
        namespace_version: str = "latest",
        compute_tier: str = "M",
        num_workers: int = 4,
    ):
        namespace = self.namespace
        runtime = "EMR"
        request_body = {"feature_names": feature_names}
        headers = {"X-API-Key": self.api_key}
        features_api_url = (
            f"{API_URL}/namespace/{namespace}/{namespace_version}/filter-server"
        )
        response = requests.post(features_api_url, json=request_body, headers=headers)
        response.raise_for_status()
        response_deser = response.json()
        inputs = {
            "feature_bundles": [
                bundle_dict for bundle_dict in response_deser.get("feature_bundles")
            ],
        }
        job = Job(
            namespace=namespace,            
            provider_region="us-east-1",
            namespace_version=namespace_version,
            runtime=Runtime(runtime),
            job_status=JobStatus.PENDING,
            inputs=inputs,
            job_type=JobType.MATERIALIZATION,
            compute_tier=ComputeTier[compute_tier],
            num_workers=num_workers,
            workspace=self.workspace,
        )

        job_data = job.to_dict()

        print(job_data)
        
        api_endpoint = (
            f"{API_URL}/jobs/{self.workspace}/{namespace}/{namespace_version}"
        )

        response = requests.post(api_endpoint, json=job_data, headers=headers)
        job_dict = response.json().get("job")
        response.raise_for_status()

        return Job.from_dict(job_dict)
