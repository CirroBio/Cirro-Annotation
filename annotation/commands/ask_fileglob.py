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

def ask_fileglob() -> str:
  
    # Would be better to list process name - but meh for now
    answers = prompt_wrapper({
        'type': 'text',
        'default': 'data/mageck/count/combined/counts.txt', #'data/mageck/count/control/*.txt', 
        'name': 'fileglob',
        'message': 'Enter the path to a file to annotate or a glob pattern to match multiple files',
    })

    return answers['fileglob']
