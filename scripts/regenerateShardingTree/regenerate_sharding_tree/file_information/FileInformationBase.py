from abc import ABC, abstractmethod
from os import PathLike
from typing import Any, AnyStr, Dict, Iterable, Union


class FileInformationBase(ABC):
    """The base class for file information implementations"""

    base_directory: Union[AnyStr, PathLike]
    """The base directory"""
    path: Union[AnyStr, PathLike]
    """The path for this object"""

    kwargs: Dict[str, Any]
    """The kwargs for the object"""

    def __init__(
        self,
        base_directory: Union[AnyStr, PathLike],
        path: Union[AnyStr, PathLike],
        **kwargs: Dict[str, Any]
    ):
        """
        Create a new FileInformationBase
        :param base_directory: The base directory
        :param path: The path in the base directory
        :param kwargs: Any additional required arguments
        """
        self.base_directory = base_directory
        self.path = path
        self.kwargs = kwargs

    @abstractmethod
    def isdir(self) -> bool:
        """
        Check if the path is a file or directory
        :return: True if the path is a directory
        """
        ...

    @abstractmethod
    def scandir(self) -> Iterable["FileInformationBase"]:
        """
        Scan a directory for other files/directories
        :return: The files and directories to check
        """
        ...

    @abstractmethod
    def getsize(self) -> int:
        """
        Get the file size
        :return: The size of the file in bytes
        """
        ...

    @abstractmethod
    def file_line_generator(self) -> Iterable[str]:
        """
        Read file lines from the file
        :return: An iterable of strings
        """
        ...
