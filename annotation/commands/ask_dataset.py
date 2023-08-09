from fnmatch import fnmatch
from pathlib import Path
from typing import List

from cirro.api.models.dataset import Dataset
from cirro.api.models.file import File
from cirro.api.models.project import Project
from cirro.cli.interactive.common_args import ask_project
from cirro.cli.interactive.utils import prompt_wrapper, InputError
from cirro.cli.models import DownloadArguments
from cirro.utils import format_date

def ask_dataset(datasets: List[Dataset]) -> str:
    if len(datasets) == 0:
        raise RuntimeWarning("No datasets available")
    sorted_datasets = sorted(datasets, key=lambda d: d.created_at, reverse=True)
    dataset_prompt = {
        'type': 'autocomplete',
        'name': 'dataset',
        'message': 'Select an exemplar dataset to use for annotation. (Press Tab to see all options)',
        'choices': [f'{dataset.name} - {dataset.id}' for dataset in sorted_datasets],
        'meta_information': {
            f'{dataset.name} - {dataset.id}': f'{format_date(dataset.created_at)}'
            for dataset in datasets
        },
        'ignore_case': True
    }
    answers = prompt_wrapper(dataset_prompt)
    choice = answers['dataset']
    # Map the answer to a dataset
    for dataset in datasets:
        if f'{dataset.name} - {dataset.id}' == choice:
            return dataset.id
    raise InputError("User must select a dataset to download")
