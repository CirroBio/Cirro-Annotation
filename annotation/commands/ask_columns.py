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

def ask_columns() -> str:
  
    # Would be better to list process name - but meh for now
    answers = prompt_wrapper({
        'type': 'text',
        'default': 'MagickFlue is used to perform quality control (QC), normalization, batch effect removal, gene hit identification and downstream functional enrichment analysis for CRISPR screens.  MAGeCK or MAGeCK-VISPR is used for primary analysis of these data.', 
        'name': 'prompt',
        'message': 'Enter information about this process',
    })

    return answers['prompt']
