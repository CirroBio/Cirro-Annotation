from fnmatch import fnmatch
from pathlib import Path
from typing import List

from cirro.api.models.dataset import Dataset
from cirro.api.models.file import File
from cirro.api.models.process import Process
from cirro.cli.interactive.common_args import ask_project
from cirro.cli.interactive.utils import prompt_wrapper, InputError
from cirro.cli.models import DownloadArguments
from cirro.utils import format_date

def ask_process(processes: List[Process]) -> str:
  
    # Would be better to list process name - but meh for now
    answers = prompt_wrapper({
        'type': 'select',
        'name': 'process',
        'message': 'Select the process to annotate',
        'choices': [
            process.id
            for process in processes
        ]
    })

    return answers['process']
