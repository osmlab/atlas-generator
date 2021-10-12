import os.path
from os import PathLike
from typing import AnyStr, Iterable, Union

from .FileInformationBase import FileInformationBase


class FileInformationLocal(FileInformationBase):
    """
    An information class for local files
    """

    def __init__(
        self,
        base_directory: Union[AnyStr, PathLike],
        path: Union[AnyStr, PathLike],
        **kwargs
    ):
        """
        Create a new info object for a local file
        :param base_directory: The base directory
        :param path: The path to use (not including the base directory)
        :param kwargs: Additional arguments, if needed
        """
        super().__init__(base_directory, path, **kwargs)

    def isdir(self) -> bool:
        return os.path.isdir(os.path.join(self.base_directory, self.path))

    def scandir(self) -> Iterable["FileInformationBase"]:
        for file in os.scandir(os.path.join(self.base_directory, self.path)):
            yield FileInformationLocal(
                self.base_directory,
                os.path.join(self.path, file.name),
                **self.kwargs
            )

    def file_line_generator(self) -> Iterable[str]:
        with open(os.path.join(self.base_directory, self.path)) as file_handle:
            line = file_handle.readline()
            while line:
                yield line.strip()
                line = file_handle.readline()

    def getsize(self) -> int:
        return os.path.getsize(os.path.join(self.base_directory, self.path))
