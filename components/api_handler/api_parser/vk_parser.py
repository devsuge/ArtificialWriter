from typing import Dict, Set, Any

import vk_api
from tqdm import tqdm
import pandas as pd

from components.api_handler.api_parser.api_parser import APIParser
from components.api_handler.common.constants import CONTENT_TYPE, POST_CONTENT

class VKParser(APIParser):
    ''
    def __init__(self,
                 token,
                 verbose: bool = False) -> None:
        ""
        super().__init__(verbose)
        self._type = 'VK_Parser'
        self._api = vk_api.vk_api.VkApi(token=token)

    
    def get_post_content(self,
                         t_id: Dict[str, str],
                         targets: Set[str],
                         count: int = 1) -> Dict[str, Any]:
        ""
        # TODO: Определние целей надо убрать мб в конфигуратор
        ctargets = {'id',
                    'date',
                    'owner_id',
                    'text',
                    'marked_as_ads',
                    'post_type',
                    'attachments',
                    'likes',
                    'views',
                    'comments'}

        if targets is not None and targets.issubset(ctargets):
            targets = targets
        else:
            targets.union(ctargets)
        
        vk = self._api.get_api()
        out_df = pd.DataFrame(columns=targets)
        setattr(out_df, CONTENT_TYPE, POST_CONTENT)

        for owner in t_id.values():
            raw_content = vk.wall.get(owner_id= owner, count=count, filter='owner')
            
            if raw_content['items']:
                post_data = pd.DataFrame(raw_content['items'],
                                         columns=targets,
                                         index=list(range(count)))
                out_df = pd.concat([out_df, post_data], ignore_index=True)
        
        out_df['date'] = pd.to_datetime(out_df['date'], unit='s')
        return out_df
