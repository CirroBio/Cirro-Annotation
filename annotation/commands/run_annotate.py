import re
import os
import glob
import pandas as pd
import json
from cirro.api.clients.portal import DataPortalClient
from cirro.cli.interactive.download_args import gather_download_arguments, gather_download_arguments_dataset, ask_dataset_files
from cirro.cli.models import ListArguments, UploadArguments, DownloadArguments
from cirro.cli.interactive.utils import get_id_from_name
from cirro.api.models.process import Executor, Process
from cirro.cli.interactive.utils import ask_yes_no, ask
from .ask_process import ask_process
from .ask_dataset import ask_dataset
from .ask_prompt import ask_prompt





def read_csv(filename):
    df = pd.read_csv(filename, sep=None, engine='python')
    df = df.infer_objects()
    numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce') 
    categorical_columns = [col for col in df.columns if df[col].nunique() < 10]
    df[categorical_columns] = df[categorical_columns].astype('category')
    return df

def get_file_columns(root_dir, files, extensions: list[str]):
    results = { 
        "columns": [],
        "files": []
    }
    csv_files = [file for file in files if any(file.endswith(ext) for ext in extensions)]
    for file in csv_files:
        df = pd.read_csv(os.path.join(root_dir, file), sep=None, engine='python')
        results["columns"].extend(df.columns)

        # Create Object With File and Columns
        results["files"].append({
            "file": file,
            "columns": df.columns.tolist()
        })


    # Trim, Lowercase, Remove Dups
    results["columns"] = [col.strip().lower() for col in results["columns"]]
    results["columns"] = list(dict.fromkeys(results["columns"]))
    return results


# List all files with relative path to root ["/dir/file.txt"]
def get_file_list(root_directory, extensions: list[str]):
    file_list = []
    for root, dirs, files in os.walk(os.path.join(root_directory, "data")):
        for file in files:

            # Skip files that are not of the correct extension
            if not any(file.endswith(ext) for ext in extensions):
                continue

            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, root_directory)
            file_list.append(relative_path)
    return file_list

# Downloads Only Extensions And If Not Input Params Passed, will prompt
def get_dataset(input_params: DownloadArguments, extensions: list[str])->str:
    cirro = DataPortalClient()
    projects = cirro.project.list()
    if len(projects) == 0:
        print("No projects available")
        return ""

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
    # Create a temp directory to download the files to if not exists
    data_dir = os.path.join(os.getcwd(), 'temp', process_id, input_params['project'], input_params['dataset'])
    os.makedirs(data_dir, exist_ok=True)
                  
    if len(os.listdir(data_dir)) == 0:
        files_to_download = cirro.dataset.get_dataset_files(input_params['project'], input_params['dataset'])

        # Filter out files that are not of the correct extension
        files_to_download = [file for file in files_to_download if any(file.name.endswith(ext) for ext in extensions)]

        cirro.dataset.download_files(project_id=input_params['project'],
                                dataset_id=input_params['dataset'],
                                download_location=data_dir,
                                files=files_to_download)
        
    return data_dir


def process_variable_columns(columns):
    column_groups = []
    while (len(columns) > 0):
        selected_columns = ask("checkbox", "Select all columns that relate to the same variable", choices=columns)
        selected_columns_name = ask("text", "Enter a column name of this variable")
        selected_columns_desc = ask("text", "Enter a column description of this variable")
        selected_value_name = ask("text", "Enter a value name of this variable")
        selected_value_desc = ask("text", "Enter a value description of this variable")
        columns = [col for col in columns if col not in selected_columns]
        column_groups.append({
            "columns": selected_columns,
            "name": selected_columns_name,
            "desc": selected_columns_desc,
            "value_name": selected_value_name,
            "value_desc": selected_value_desc
        })
    return column_groups


# Generates Regex Pattern From Uppercased Token [COLUMN_NAME]
def process_variable_files(files):

    file_groups = []
    token_metadata = []
    while (len(files) > 0):
        file = files[0]
        print("Replace the part(s) of this path that are variable with a [column_name] wrapped in square brackets")
        print(file)
        file_pattern = ask("text", "", default=file)
        token_names = re.findall(r'\[(\w+)\]', file_pattern)
        regex_pattern = file_pattern
        for token_name in token_names:
            regex_pattern = regex_pattern.replace(f'[{token_name}]', r'(?P<{}>[^/]+)'.format(token_name))

        matches = []
        for file in files:
            match = re.match(regex_pattern, file)
            if match:
                matches.append(file)
        
        files = [file for file in files if file not in matches]
        
        if (len(matches) > 0):
            print(str(len(matches)) + " files matched")
            for token in token_names:

                # If Token Already Exists In Metadata Then Use It As Defaults To Prompt
                if any(token == obj['token'] for obj in token_metadata):
                    token_metadata_obj = next(obj for obj in token_metadata if obj['token'] == token)
                    token_metadata_name = token_metadata_obj['name']
                    token_metadata_desc = token_metadata_obj['desc']
                else:
                    token_metadata_name = ""
                    token_metadata_desc = ""

                token_metadata.append({
                    "token": token,
                    "name": ask("text", f"Enter a name for this {token}", default=token_metadata_name),
                    "desc": ask("text", f"Enter a description for this {token}", default=token_metadata_desc),
                })

            file_groups.append({
                "name": ask("text", "Enter a name for this group of files"),
                "desc": ask("text", "Enter a description for this group of files"),
                "tokens": token_metadata,
                "example": matches,
                "pattern": file_pattern,
                "regex": regex_pattern,
            })

    return file_groups


def generate_variable_file_manifest(files, columns_mapping, columns_variable):
    transforms = []
    for file in files:


        # Filter Columns That Don't exist in columns_mapping
        columns_standard_in_file = [col for col in file["columns"] if col in columns_mapping]
        columns_standard_in_file = [columns_mapping[col] for col in columns_standard_in_file]
    

        transform = {
            "command": "hot.Parquet",
            "params": {
                "source": file['pattern'].replace('data', '$data_directory'),
                "target": ask("text", f"Enter a file name for {file['pattern']}: ", default=file['pattern'].split('/')[-1]),
                "name": file['name'],
                "desc": file['desc'],
                "cols": columns_standard_in_file,
                "concat": file['tokens']
            }
        }

        # Find the object in columns_variable that has the non-standard columns in it
        columns_variable_in_file = [obj for obj in columns_variable if all(col in obj['columns'] for col in file["columns"])]
        if len(columns_variable_in_file) > 0:
            transform["params"]["melt"] = {
                "key": {
                    "name": columns_variable_in_file[0]['name'],
                    "desc": columns_variable_in_file[0]['desc'],
                },
                "value": {
                    "name": columns_variable_in_file[0]['value_name'],
                    "desc": columns_variable_in_file[0]['value_desc'],
                }
            }
        
        transforms.append(transform)

    return transforms

    
def generate_standard_file_manifest(files, columns_mapping, columns_variable):
     transforms = []
     for file in files:
        # Filter Columns That Don't exist in columns_mapping
        columns_standard_in_file = [col for col in file["columns"] if col in columns_mapping]
        columns_standard_in_file = [columns_mapping[col] for col in columns_standard_in_file]
    

        transform = {
            "command": "hot.Parquet",
            "params": {
                "source": file['file'].replace('data', '$data_directory'),
                "target": ask("text", f"Enter a file name for {file['file']}: ", default=file['file'].split('/')[-1]),
                "name": ask("text", f"Enter a friendly name for {file['file']}: "),
                "desc": ask("text", f"Enter a description for {file['file']}: "),
                "cols": columns_standard_in_file
            }
        }

        # Find the object in columns_variable that has the non-standard columns in it
        columns_variable_in_file = [obj for obj in columns_variable if all(col in obj['columns'] for col in file["columns"])]
        if len(columns_variable_in_file) > 0:
            transform["params"]["melt"] = {
                "key": {
                    "name": columns_variable_in_file[0]['name'],
                    "desc": columns_variable_in_file[0]['desc'],
                },
                "value": {
                    "name": columns_variable_in_file[0]['value_name'],
                    "desc": columns_variable_in_file[0]['value_desc'],
                }
            }
        
        transforms.append(transform)
        
     return transforms
        


def run_annotate(input_params: DownloadArguments):
    
    extensions = ['.txt','.csv','.tsv','.txt.gz','.csv.gz','.tsv.gz']

    dataset_directory = get_dataset(input_params, extensions)

    # Process Files
    files = get_file_list(dataset_directory, extensions)
    files.sort()
    files_variable = ask("checkbox", "Select all files that vary by run?", choices=files)
    files_standard = [file for file in files if file not in files_variable]
    files_variable = process_variable_files(files_variable)
    file_columns = get_file_columns(dataset_directory, files, extensions)

    
    # Map Values of columns_standard to a variable
    files_mapping = {obj['file']: obj for obj in file_columns["files"]}
    files_standard = [files_mapping[value] for value in files_standard]
    for value in files_variable:
        value['columns']  = files_mapping[value['example'][0]]['columns']

    columns = file_columns["columns"]
    columns.sort()
    columns_variable = ask("checkbox", "Select all columns that vary by run?", choices=columns)
    columns_standard = [col for col in columns if col not in columns_variable]

    # Load Fields.json file
    with open(os.path.join(os.getcwd(),"json/example/gpt/fields.json"), 'r') as file:
        col_metadata = json.load(file)
    columns_mapping = {obj['col']: obj for obj in col_metadata}

    # Generate A List Of Standard Columns Which Do Not Exist In Columns Mapping
    columns_missing = [col for col in columns_standard if col not in columns_mapping]


    # If Columns Missing, Ask User To Map Them
    if len(columns_missing) > 0:
        print("The following columns are missing from the fields.json file")
        print(columns_missing)
        print("Please map them to a variable")
        for col in columns_missing:
            columns_mapping[col] = {
                "col": col,
                "name": ask("text", f"Enter a name for this {col}: "),
                "desc": ask("text", f"Enter a description for this {col}: "),
            }

        # Add New Columns To Fields.json File
        col_metadata.extend(columns_mapping.values())
        with open(os.path.join(os.getcwd(),"json/example/gpt/fields.json"), 'w') as file:
            json.dump(col_metadata, file, indent=4)

    columns_standard = [columns_mapping[value] for value in columns_standard]
    columns_variable = process_variable_columns(columns_variable)
    standard_metadata = generate_standard_file_manifest(files_standard, columns_mapping, columns_variable)
    variable_metadata = generate_variable_file_manifest(files_variable, columns_mapping, columns_variable)
   
    metadata = {
       "commands": [
              standard_metadata,
                variable_metadata
         ]
    }

    # Split The dataset directory by / and determine the index of the directory called "temp"
    temp_index = dataset_directory.split('/').index('temp') + 1
    dataset_directory = '/'.join(dataset_directory.split('/')[0:temp_index])

    with open(os.path.join(dataset_directory, "manifest.json"), 'w') as f:
        json.dump(metadata, f, indent=4)
