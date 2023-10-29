from typing import Iterable, Dict, Union, List
import json
from requests import get
from http import HTTPStatus

StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


def get_paginated_dataset(filters: FiltersType, structure: StructureType, as_csv: bool = False) -> APIResponseType:
    '''
    Extracts paginated data by requesting all of the pages
    and combining the results.

    Parameters
    ----------
    filters: Iterable[str]
        API filters. See the API documentations for additional
        information.

    structure: Dict[str, Union[dict, str]]
        Structure parameter. See the API documentations for
        additional information.
        as_csv: bool
        Return the data as CSV. [default: ``False``]

    Returns
    -------
    Union[List[StructureType], str]
        Comprehensive list of dictionaries containing all the data for
        the given ``filters`` and ``structure``.
    '''
    endpoint = 'https://api.coronavirus.data.gov.uk/v1/data'
    
    api_params = {'filters': str.join(';', filters),
                  'structure': json.dumps(structure, separators = (',', ':')),
                  'format': 'json' if not as_csv else 'csv'
                 }
    data = list()
    page_number = 1
    while True:
        # add page number to query params
        api_params['page'] = page_number
        response = get(endpoint, params = api_params, timeout = 10)
        if response.status_code >= HTTPStatus.BAD_REQUEST:
            raise RuntimeError(f'Request failed: {response.text}')
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break
        if as_csv:
            csv_content = response.content.decode()
            # Removing CSV header (column names) where page number is greater than 1
            if page_number > 1:
                data_lines = csv_content.split('\n')[1:]
                csv_content = str.join('\n', data_lines)
            data.append(csv_content.strip())
            page_number += 1
            continue
        current_data = response.json()
        page_data: List[StructureType] = current_data['data']
        data.extend(page_data)
        # The "next" attribute in "pagination" will be `None` when we reach the end
        if not current_data['pagination']['next']:
            break
        page_number += 1
    if not as_csv:
        return data
    # concatenating CSV pages
    return str.join('\n', data)