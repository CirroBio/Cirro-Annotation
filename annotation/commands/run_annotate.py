
from cirro.api.clients.portal import DataPortalClient
from cirro.cli.interactive.download_args import gather_download_arguments, gather_download_arguments_dataset, ask_dataset_files
from cirro.cli.models import ListArguments, UploadArguments, DownloadArguments
from cirro.cli.interactive.utils import get_id_from_name
from cirro.api.models.process import Executor, Process
from .ask_process import ask_process
from .ask_dataset import ask_dataset

def run_annotate(input_params: DownloadArguments):
    """Desc"""
    cirro = DataPortalClient()
    projects = cirro.project.list()
    
    if len(projects) == 0:
        print("No projects available")
        return

    files_to_download = None
    input_params = gather_download_arguments(input_params, projects)
    input_params['project'] = get_id_from_name(projects, input_params['project'])
    datasets = cirro.dataset.find_by_project(input_params['project'])
    processes = cirro.process.list(process_type=Executor.NEXTFLOW)
    unique_process_ids = set(dataset.process_id for dataset in datasets)
    unique_processes = [process for process in processes if process.id in unique_process_ids]
    process_id = ask_process(processes=unique_processes)
    process_datasets = [dataset for dataset in datasets if dataset.process_id == process_id]
    input_params['dataset'] = ask_dataset(process_datasets)

    # Print Hello World In Spansh
    
    
    

    files = cirro.dataset.get_dataset_files(input_params['project'], input_params['dataset'])

    


    # Retrieve the processes 
    



    datasets[0].process_id


    x = ""

    

    # datasets = cirro.dataset.find_by_project(input_params['project'])
    # input_params = gather_download_arguments_dataset(input_params, datasets)
    # dataset_files = cirro.dataset.get_dataset_files(input_params['project'], input_params['dataset'])
    # files_to_download = ask_dataset_files(dataset_files)

    # dataset_params = {
    #     'project': get_id_from_name(projects, input_params['project']),
    #     'dataset': input_params['dataset']
    # }

    # cirro.dataset.download_files(project_id=dataset_params['project'],
    #                              dataset_id=dataset_params['dataset'],
    #                              download_location=input_params['data_directory'],
    #                              files=files_to_download)