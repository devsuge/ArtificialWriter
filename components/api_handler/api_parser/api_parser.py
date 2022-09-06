"""This script contains abstract class for api_parser scripts."""
from abc import ABC
from abc import abstractmethod
import datetime as dt
from pathlib import Path
from typing import Any, Dict, Set


from tqdm import tqdm
import pandas as pd

from components.api_handler.common.constants import CONTENT_TYPE, UNKNOWN_CONTENT
from components.common.constants import DATE_FORMAT, DF_FORMAT, SPLIT, SUB_SPLIT, FILE_NAME


class APIParser(ABC):
    """This class is the abstract class for api_paresrs."""

    def __init__(self, token: Any = None, verbose: bool = False) -> None:
        """Creates an instance of the class.

        Args:
            token (Any, optional): Contains api token. Defaults to None.
            verbose (bool, optional): Verbose flag. Defaults to False.
        """
        self._type = 'ABC_Parser'
        self._api_url = None
        self._api_version = None
        self._token = token
        self._verbose = verbose

    @abstractmethod
    def get_post_content(self,
                         g_id: Dict[str, str],
                         targets: Set[str],
                         count: int = 1) -> Dict[str, Any]:
        """Abstract method for post content parsing methods."""

    @classmethod
    def save_data(cls, df: pd.DataFrame, out_path: Path, just_save: bool = False) -> None:
        """Overall save method for subclasses.

        Args:
            df (pd.DataFrame): DataFrame to be saved.
            out_path (Path): Path to save directory or file.
            just_save (bool, optional): Flag reflecting simple saving without adding subfolders. Defaults to False.

        Returns:
            None
        """
        out_path = out_path.resolve()

        # Check custom attr for DataFrame
        if hasattr(df, CONTENT_TYPE):
            content = getattr(df, CONTENT_TYPE)
        else:
            content = UNKNOWN_CONTENT

        #First, it checks whether the data needs to be added to the file.
        if out_path.is_file() and out_path.name.split(SPLIT)[-1] == f'{content}.{DF_FORMAT}':
            df = pd.concat([pd.DataFrame(Path), df])

            parser_types = out_path.name.split(SPLIT)[0].split(SUB_SPLIT)
            if cls._type not in parser_types:
                parser_types.append(cls._type)
                out_path.rename(out_path.parent /
                                FILE_NAME.format(classtype=parser_types.join(SUB_SPLIT),
                                                 content=content,
                                                 fileformat=DF_FORMAT))
        else:
            if not just_save:
                out_path = out_path / f'{dt.date.today().strftime(DATE_FORMAT)}'
            out_path.mkdir(parents=True, exist_ok=True)
            out_path = out_path / FILE_NAME.format(classtype=cls._type,
                                                   content=content,
                                                   fileformat=DF_FORMAT)

        return df.to_csv(out_path, index=False)
