import os
from os import PathLike
from typing import TYPE_CHECKING, AnyStr, ClassVar, Dict, Iterable, Union

import boto3
import botocore.exceptions
from tqdm import tqdm

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Bucket, ObjectSummary
else:
    Bucket = object
    ObjectSummary = object
    S3ServiceResource = object

from tempfile import NamedTemporaryFile

from .FileInformationBase import FileInformationBase


class FileInformationS3Directory(FileInformationBase):
    """
    A S3 class holding directory information.
    Only used for caching ObjectSummary information for speed/cost reasons.
    """

    _mapping: ClassVar[
        Dict[Union[AnyStr, PathLike], "FileInformationS3Directory"]
    ] = {}
    """A mapping of paths to FileInformationS3Directory objects"""

    _file_mapping: Dict[Union[AnyStr, PathLike], ObjectSummary]
    """A mapping of paths to objects"""

    client: S3ServiceResource = None
    """The S3 client"""
    bucket: Bucket = None
    """The S3 bucket"""
    s3_object: ObjectSummary = None
    """The object summary"""
    _isdir: bool
    """True if directory. Lazily calculated"""

    @classmethod
    def get_directory_search(
        cls,
        path: Union[AnyStr, PathLike],
        client: S3ServiceResource = None,
        bucket: Bucket = None,
        **kwargs,
    ) -> "FileInformationS3Directory":
        # Remove any trailing /, if they exist
        path = path.rstrip("/")
        if bucket and not path.startswith("s3://" + bucket.name):
            path = "s3://" + bucket.name + "/" + path[len("s3://") :]
        information = cls._mapping.get(path)
        if information:
            return information
        information = FileInformationS3Directory(
            path, None, client=client, bucket=bucket, **kwargs
        )
        cls._mapping[path] = information
        return information

    def __init__(
        self,
        base_directory: Union[AnyStr, PathLike],
        path: Union[None, AnyStr, PathLike],
        client: S3ServiceResource = None,
        bucket: Bucket = None,
        s3_object: ObjectSummary = None,
        **kwargs,
    ):
        """
        Create a new S3 file information object.

        See
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        for authentication options. If passing parameters, aws_access_key_id
        and aws_secret_access_key are required in an s3 variable (see kwargs).

        :param base_directory: The base directory, should look like
                               "s3://[bucket]/[path]" (either [bucket] or
                               bucket_name must be specified
        :param path: The object path, should look like "[object]".
        :param client: The s3 client to use. If not set, a new client is
                       created.
        :param bucket: The s3 bucket to use. If not set, generated from
                       base_directory
        :param s3_object: The s3 object for this file. If not set, one is
                          created.
        :param kwargs: {s3: {region: str, bucket_name: Optional[str],
                        region_name: Optional[str],
                        api_version: Optional[str], use_ssl: Optional[bool],
                        verify: Optional[bool], endpoint_url: Optional[str],
                        aws_secret_access_key: Optional[str],
                        aws_session_token: Optional[str],
                        aws_access_key_id: Optional[str],
                        config: Optional[Config]}}
        """
        super().__init__(base_directory, path, **kwargs)
        s3 = kwargs.get("s3") if kwargs and "s3" in kwargs else {}
        if client:
            self.client = client
        else:
            self.client = boto3.resource(
                service_name="s3",
                region_name=s3.get("region_name"),
                api_version=s3.get("api_version"),
                use_ssl=s3.get("use_ssl"),
                verify=s3.get("verify"),
                endpoint_url=s3.get("endpoint_url"),
                config=s3.get("config"),
                aws_secret_access_key=s3.get("aws_secret_access_key"),
                aws_session_token=s3.get("aws_session_token"),
                aws_access_key_id=s3.get("aws_access_key_id"),
            )
        path_str = str(base_directory)
        if not path_str.startswith("s3://"):
            raise ValueError("An s3:// path is required")
        path_str = base_directory[len("s3://") :]
        if bucket:
            self.bucket = bucket
        else:
            self.bucket = self.client.Bucket(
                s3.get("bucket_name")
                if "bucket_name" in s3
                else path_str[: path_str.index("/")]
            )
        self.base_directory = (
            path_str[len(self.bucket.name) + 1 :]
            if path_str.startswith(self.bucket.name)
            else path_str
        )
        if s3_object:
            self.s3_object = s3_object
        else:
            self.s3_object = self.client.ObjectSummary(
                self.bucket.name,
                self.base_directory + "/" + str(self.path)
                if self.path
                else self.base_directory,
            )

    def scandir(
        self, path: Union[AnyStr, PathLike] = None
    ) -> Iterable["FileInformationBase"]:
        if not hasattr(self, "_file_mapping"):
            self._file_mapping = {}
            for s3_obj in tqdm(
                self.bucket.objects.filter(Prefix=self.base_directory),
                desc=f"s3: listing {self.base_directory}",
                leave=None,
            ):
                if s3_obj.size:
                    self._file_mapping[s3_obj.key] = s3_obj

        for s3_key in self._file_mapping:
            if str(path) in s3_key:
                s3_obj = self._file_mapping.get(s3_key)
                yield FileInformationS3(
                    f"s3://{s3_obj.bucket_name}/{self.base_directory}",
                    s3_obj.key[len(self.base_directory) + 1 :],
                    client=self.client,
                    bucket=self.bucket,
                    s3_object=s3_obj,
                    **self.kwargs,
                )

    def getsize(self) -> int:
        raise NotImplementedError

    def file_line_generator(self) -> Iterable[str]:
        raise NotImplementedError

    def isdir(self) -> bool:
        return True


class FileInformationS3(FileInformationS3Directory):
    """
    S3 specific file information
    """

    _isdir: bool
    """True if directory. Lazily calcuated"""

    def __init__(
        self,
        base_directory: Union[AnyStr, PathLike],
        path: Union[None, AnyStr, PathLike],
        client: S3ServiceResource = None,
        bucket: Bucket = None,
        s3_object: ObjectSummary = None,
        **kwargs,
    ):
        """
        Create a new S3 file information object.

        See
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
        for authentication options. If passing parameters, aws_access_key_id
        and aws_secret_access_key are required in an s3 variable (see kwargs).

        :param base_directory: The base directory, should look like
                               "s3://[bucket]/[path]" (either [bucket] or
                               bucket_name must be specified
        :param path: The object path, should look like "[object]".
        :param client: The s3 client to use. If not set, a new client is
                       created.
        :param bucket: The s3 bucket to use. If not set, generated from
                       base_directory
        :param s3_object: The s3 object for this file. If not set, one is
                          created.
        :param kwargs: {s3: {region: str, bucket_name: Optional[str],
                        region_name: Optional[str],
                        api_version: Optional[str], use_ssl: Optional[bool],
                        verify: Optional[bool], endpoint_url: Optional[str],
                        aws_secret_access_key: Optional[str],
                        aws_session_token: Optional[str],
                        aws_access_key_id: Optional[str],
                        config: Optional[Config]}}
        """
        super().__init__(
            base_directory,
            path,
            client=client,
            bucket=bucket,
            s3_object=s3_object,
            **kwargs,
        )

    def isdir(self) -> bool:
        if hasattr(self, "_isdir"):
            return self._isdir

        self._isdir = (
            self.s3_object is None
            or self.s3_object.size == 0
            and list(self.bucket.objects.filter(Prefix=self.path).limit(1))
        )
        return self._isdir

    def scandir(self) -> Iterable["FileInformationBase"]:
        for s3_obj in FileInformationS3Directory.get_directory_search(
            "s3://" + self.base_directory,
            client=self.client,
            bucket=self.bucket,
            **self.kwargs,
        ).scandir(path=self.path):
            yield s3_obj

    def getsize(self) -> int:
        try:
            return self.s3_object.size if self.s3_object else 0
        except botocore.exceptions.ClientError:
            self.s3_object = None
            return 0

    def file_line_generator(self) -> Iterable[str]:
        temp_file = NamedTemporaryFile(delete=False)
        try:
            with open(temp_file.name, "wb") as file_handle:
                self.s3_object.Object().download_fileobj(file_handle)
            with open(temp_file.name) as file_handle:
                line = file_handle.readline()
                while line:
                    yield line
                    line = file_handle.readline()
        finally:
            os.remove(temp_file.name)
