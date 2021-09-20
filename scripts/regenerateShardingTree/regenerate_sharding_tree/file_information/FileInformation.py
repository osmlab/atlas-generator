from typing import Any, AnyStr, Callable, Dict, Union

from .FileInformationBase import FileInformationBase
from .FileInformationLocal import FileInformationLocal

try:
    from .FileInformationS3 import FileInformationS3Directory
except ModuleNotFoundError:
    pass
from os import PathLike


class FileInformation(object):
    """A class to get file information from different sources"""

    @staticmethod
    def get_file_information(
        base_directory: Union[AnyStr, PathLike],
        filename: Union[AnyStr, PathLike],
        clazz: Callable[
            [str, Dict[str, Any]], FileInformationBase
        ] = FileInformationBase,
        **kwargs
    ) -> FileInformationBase:
        """
        Get an object for reading file information
        :param base_directory: The base file path
        :param filename: The file path (not including the base_directory)
        :param clazz: The class to use (autodetect should work most of the
                      time)
        :param kwargs: Any arguments necessary for the file information
        :return: An object to get file information with
        """
        if clazz != FileInformationBase:
            return clazz(base_directory, filename, **kwargs)

        if str(base_directory).startswith("s3://"):
            for (
                file_information
            ) in FileInformationS3Directory.get_directory_search(
                base_directory, **kwargs
            ).scandir(
                filename
            ):
                return file_information
        return FileInformationLocal(base_directory, filename, **kwargs)
