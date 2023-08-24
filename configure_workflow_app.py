import io
import json
import threading
from time import sleep
from typing import Dict, List
import zipfile
from cirro import DataPortal
from cirro.api.auth.oauth_client import ClientAuth
from cirro.api.config import AppConfig
from cirro.api.clients.portal import DataPortalClient
import streamlit as st
from streamlit.delta_generator import DeltaGenerator
from streamlit.runtime.scriptrunner import get_script_run_ctx
from streamlit.runtime.scriptrunner import script_run_context


def session_cache(func):
    def inner(*args, **kwargs):

        # Get the session context, which has a unique ID element
        ctx = get_script_run_ctx()

        # Define a cache key based on the function name and arguments
        cache_key = ".".join([
            str(ctx.session_id),
            func.__name__,
            ".".join(map(str, args)),
            ".".join([
                f"{k}={v}"
                for k, v in kwargs.items()
            ])
        ])

        # If the value has not been computed
        if st.session_state.get(cache_key) is None:
            # Compute it
            st.session_state[cache_key] = func(
                *args,
                **kwargs
            )

        # Return that value
        return st.session_state[cache_key]

    return inner


def cirro_login(login_empty: DeltaGenerator):
    # If we have not logged in yet
    if st.session_state.get('DataPortal') is None:

        # Connect to Cirro - capturing the login URL
        auth_io = io.StringIO()
        cirro_login_thread = threading.Thread(
            target=cirro_login_sub,
            args=(auth_io,)
        )
        script_run_context.add_script_run_ctx(cirro_login_thread)

        cirro_login_thread.start()

        login_string = auth_io.getvalue()

        while len(login_string) == 0 and cirro_login_thread.is_alive():
            sleep(1)
            login_string = auth_io.getvalue()

        login_empty.write(login_string)
        cirro_login_thread.join()

    else:
        login_empty.empty()

    msg = "Error: Could not log in to Cirro"
    assert st.session_state.get('DataPortal') is not None, msg


def cirro_login_sub(auth_io: io.StringIO):

    app_config = AppConfig()

    st.session_state['DataPortal-auth_info'] = ClientAuth(
        region=app_config.region,
        client_id=app_config.client_id,
        auth_endpoint=app_config.auth_endpoint,
        enable_cache=False,
        auth_io=auth_io
    )

    st.session_state['DataPortal-client'] = DataPortalClient(
        auth_info=st.session_state['DataPortal-auth_info']
    )
    st.session_state['DataPortal'] = DataPortal(
        client=st.session_state['DataPortal-client']
    )


def list_datasets_in_project(project_name):

    # Connect to Cirro
    portal = st.session_state['DataPortal']

    # Access the project
    project = portal.get_project_by_name(project_name)

    # Get the list of datasets available (using their easily-readable names)
    return [""] + [ds.name for ds in project.list_datasets()]


@session_cache
def list_processes() -> List[str]:

    # Connect to Cirro
    portal: DataPortal = st.session_state['DataPortal']

    # List the projects available
    process_list = portal.list_processes()

    # Return the list of processes available
    # (using their easily-readable names)
    process_list = [
        f"{process.name} ({process.id})"
        for process in process_list
    ]
    process_list.sort()
    return process_list


@session_cache
def list_projects() -> List[str]:

    # Connect to Cirro
    portal = st.session_state['DataPortal']

    # List the projects available
    project_list = portal.list_projects()

    # Return the list of projects available (using their easily-readable names)
    return [proj.name for proj in project_list]


@session_cache
def get_dataset(project_name, dataset_name):
    """Return a Cirro Dataset object."""

    # Connect to Cirro
    portal = st.session_state['DataPortal']

    # Access the project
    project = portal.get_project_by_name(project_name)

    # Get the dataset
    return project.get_dataset_by_name(dataset_name)


@session_cache
def list_files_in_dataset(project_name, dataset_name):
    """Return a list of files in a dataset."""

    return [
        f.name
        for f in get_dataset(project_name, dataset_name).list_files()
    ]


@session_cache
def read_csv(project_name, dataset_name, fn, **kwargs):
    """Read a CSV from a dataset in Cirro."""

    return (
        get_dataset(project_name, dataset_name)
        .list_files()
        .get_by_name(f"data/{fn}")
        .read_csv(**kwargs)
    )


class WorkflowConfigElement:
    """Parent class for workflow configuration elements."""

    def __init__(self):
        pass

    def load(self, config: dict) -> None:
        """
        Set up attributes based on the contents
        of the configuration JSON
        """
        pass

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """
        pass

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """
        pass


class SourceConfig(WorkflowConfigElement):

    root_kwargs: dict
    code_kwargs: dict

    def __init__(self):
        self._id = "dynamo"
        self.root_kwargs = {
            "id": "unique-workflow-id",
            "name": "My Workflow Name",
            "desc": "Description of my workflow",
            "executor": "NEXTFLOW",
            "documentationUrl": "",
            "childProcessIds": [],
            "parentProcessIds": []
        }
        self.code_kwargs = {
            "repository": "GITHUBPUBLIC",
            "script": "main.nf",
            "uri": "org/repo",
            "version": "main"
        }

    def load(self, config: dict) -> None:

        for kw, default in self.root_kwargs.items():
            self.__dict__[kw] = config[self._id].get(kw, default)

        for kw, default in self.code_kwargs.items():
            self.__dict__[kw] = config[self._id]["code"].get(kw, default)

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """

        for kw in self.root_kwargs.keys():
            val = self.__dict__.get(kw)
            config[self._id][kw] = val.upper() if kw == "executor" else val

        for kw in self.code_kwargs.keys():
            config[self._id]["code"][kw] = self.__dict__.get(kw)

    def update_value(self, config: 'WorkflowConfig', kw: str):

        # Get the update value
        key = f"{self._id}.{kw}.{st.session_state['form_ix']}"
        val = st.session_state[key]
        self.__dict__[kw] = val
        config.save_config()
        config.reset()

    def input_kwargs(self, config: 'WorkflowConfig', kw: str):
        return dict(
            key=f"{self._id}.{kw}.{st.session_state['form_ix']}",
            on_change=self.update_value,
            args=(config, kw)
        )

    def input_process_kwargs(self, config: 'WorkflowConfig', kw: str):
        return dict(
            key=f"{self._id}.{kw}.{st.session_state['form_ix']}",
            on_change=self.update_process_list,
            args=(config, kw)
        )

    def get_process_id(self, long_name: str):
        return long_name.rsplit(" (", 1)[-1].rstrip(")")

    def update_process_list(self, config: 'WorkflowConfig', kw: str):
        key = f"{self._id}.{kw}.{st.session_state['form_ix']}"
        process_list = st.session_state[key]

        # Get the process IDs for each process
        process_list = [
            self.get_process_id(process)
            for process in process_list
        ]

        self.__dict__[kw] = process_list
        config.save_config()
        config.reset()

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """

        expander = config.form_container.expander(
            "Workflow Information",
            expanded=True
        )

        expander.text_input(
            "Workflow ID",
            self.id,
            help="Must be all lowercase alphanumeric with dashes",
            **self.input_kwargs(config, "id")
        )

        expander.text_input(
            "Workflow Name",
            value=self.name,
            help="Short name used to display the workflow in a list",
            **self.input_kwargs(config, "name")
        )

        expander.text_input(
            "Workflow Description",
            value=self.desc,
            help="Longer description providing more details on the workflow (8-15 words)",
            **self.input_kwargs(config, "desc")
        )

        expander.radio(
            "Workflow Executor",
            ["Nextflow", "Cromwell"],
            ["Nextflow", "Cromwell"].index(self.executor.title()),
            **self.input_kwargs(config, "executor")
        ).upper()

        expander.text_input(
            "Workflow Repository (GitHub)",
            help="For private workflows, make sure to [install the CirroBio app](https://github.com/apps/cirro-data-portal) to provide access",
            value=self.uri,
            **self.input_kwargs(config, "uri")
        )

        expander.text_input(
            "Workflow Entrypoint",
            value=self.script,
            help="Script from the repository used to launch the workflow",
            **self.input_kwargs(config, "script")
        )

        expander.text_input(
            "Repository Version",
            value=self.version,
            help="Supports branch names, commits, tags, and releases.",
            **self.input_kwargs(config, "version")
        )

        expander.selectbox(
            "Public / Private",
            ["GITHUBPUBLIC", "GITHUBPRIVATE"],
            ["GITHUBPUBLIC", "GITHUBPRIVATE"].index(self.repository),
            help="Supports branch names, commits, tags, and releases.",
            **self.input_kwargs(config, "repository")
        )

        expander.multiselect(
            "Parent Processes",
            list_processes(),
            [
                process for process in list_processes()
                if self.get_process_id(process) in self.parentProcessIds
            ],
            help="Datasets produced by parent processes can be used as inputs to run this workflow",
            **self.input_process_kwargs(config, "parentProcessIds")
        )

        expander.multiselect(
            "Child Processes",
            list_processes(),
            [
                process for process in list_processes()
                if self.get_process_id(process) in self.childProcessIds
            ],
            help="Child processes can be run on the datasets produced as outputs by this workflow",
            **self.input_process_kwargs(config, "childProcessIds")
        )


class ParamsConfig(WorkflowConfigElement):

    def load(self, config: dict) -> None:
        """
        Set up attributes based on the contents
        of the configuration JSON
        """
        pass

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """
        pass

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """
        pass


class OutputsConfig(WorkflowConfigElement):

    def load(self, config: dict) -> None:
        """
        Set up attributes based on the contents
        of the configuration JSON
        """
        pass

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """
        pass

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """
        pass


class PreprocessConfig(WorkflowConfigElement):

    def load(self, config: dict) -> None:
        """
        Set up attributes based on the contents
        of the configuration JSON
        """
        self.preprocess = config["preprocess"]

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """
        config["preprocess"] = self.preprocess

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """
        pass


class ComputeConfig(WorkflowConfigElement):

    def load(self, config: dict) -> None:
        """
        Set up attributes based on the contents
        of the configuration JSON
        """
        self.compute = config["compute"]

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """
        config["compute"] = self.compute

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """
        pass


class WorkflowConfig:
    """Workflow configuration object."""

    elements: List[WorkflowConfigElement]

    def __init__(self):

        # Set up configuration elements, each
        # of which is a WorkflowConfigElement
        self.elements = [
            SourceConfig(),
            ParamsConfig(),
            OutputsConfig(),
            PreprocessConfig(),
            ComputeConfig(),
        ]

    def save_config(self) -> None:
        """Save a new copy of the config in the session state."""

        # Update the session state
        st.session_state["config"] = self.format_config()

    def format_config(self) -> dict:
        """Generate a config file based on the app state."""

        # Make a blank copy
        config = {
            kw: default
            for kw, default in [
                ("dynamo", dict()), 
                ("form", dict()), 
                ("input", dict()), 
                ("output", dict()), 
                ("compute", ""), 
                ("preprocess", "")
            ]
        }
        config["dynamo"]["code"] = dict()

        # Populate the config based on the state of the form
        for element in self.elements:
            element.dump(config)

        return config

    def load_config(self) -> dict:
        """
        Load the configuration from the session state,
        filling in a default if not present.
        """

        return st.session_state.get(
            "config",
            dict(
                dynamo=dict(
                    code=dict()
                ),
                form=dict(form=dict(), ui=dict()),
                input=dict(),
                output=dict(),
                compute="",
                preprocess=""
            )
        )

    def serve(self):
        """Launch an interactive display allowing the user to configure the workflow."""

        # Set up the page
        st.set_page_config(
            page_title="Cirro - Workflow Configuration",
            page_icon="https://cirro.bio/favicon-32x32.png"
        )
        st.header("Cirro - Workflow Configuration")

        # Log in to Cirro
        login_empty = st.empty()
        cirro_login(login_empty)
        login_empty.empty()

        # Set up tabs for the form and all generated elements
        tab_names = [
            "Builder",
            "Dynamo",
            "Form",
            "Input",
            "Compute",
            "Preprocess",
            "Output"
        ]
        self.tabs = dict(zip(tab_names, st.tabs(tab_names)))

        # Set up an empty in each of the tabs
        self.tabs_empty: Dict[str, DeltaGenerator] = {
            kw: tab.empty()
            for kw, tab in self.tabs.items()
        }

        # Let the user upload files
        self.add_file_uploader()

        # Set up an empty which will be populated with the "Download All" button
        self.download_all_empty = st.sidebar.empty()

        # Populate the form and downloads
        self.reset()

    def reset(self):

        # Increment the index used for making unique element IDs
        st.session_state["form_ix"] = st.session_state.get("form_ix", -1) + 1

        # Populate the form
        self.populate_form()

        # Set up the download options
        self.populate_downloads()

    def populate_form(self):
        """Generate the form based on the configuration elements."""

        # Set up the container
        self.form_container = self.tabs_empty["Builder"].container()

        # Get the configuration from the session state
        config = self.load_config()

        # Iterate over each of the display elements
        for element in self.elements:
            # Load attributes from the configuration
            element.load(config)
            # Serve the interactivity of the configuration
            element.serve(self)

        self.save_config()

    def populate_downloads(self):
        """Populate the options for downloading files"""

        # Create a zip file with all of the files
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "a") as zip_file:

            for kw, val in self.format_config().items():
                prefix = "" if kw == "preprocess" else "process-"
                ext = dict(
                    preprocess="py",
                    compute="config"
                ).get(kw, "json")
                file_name = f"{prefix}{kw}.{ext}"

                # Format the text of the element
                text = val if isinstance(val, str) else json.dumps(val, indent=4, sort_keys=True)

                # Add to the zip file
                zip_file.writestr(file_name, text)

                # Replace the contents of the tab
                cont = self.tabs_empty[kw.title()].container()

                # Add a download button in the tab
                cont.download_button(
                    f"Download {file_name}",
                    text,
                    file_name=file_name,
                    key=f"download.{kw}.{st.session_state['form_ix']}"
                )

                # Print the element in the tab
                cont.text(text)

        # Let the user download all files as a zip
        self.download_all_empty.download_button(
            "Download all (ZIP)",
            zip_buffer,
            file_name="cirro-configuration.zip",
            key=f"download.all.{st.session_state['form_ix']}"
        )

    def add_file_uploader(self):

        # Let the user upload files
        upload_files = st.sidebar.expander("Upload Files", expanded=True)
        upload_files.file_uploader(
            "Upload Configuration Files",
            accept_multiple_files=True,
            key="uploaded_files"
        )
        upload_files.button(
            "Load Configuration from Files",
            on_click=self.load_from_uploaded_files
        )

    def load_from_uploaded_files(self):
        """Load configuration from uploaded files."""
        # Get the configuration from the session state
        config = st.session_state.get("config")
        if config is None:
            return
        modified = False

        for file in st.session_state.get("uploaded_files", []):
            if not file.name.startswith("process-"):
                if file.name == "preprocess.py":
                    config["preprocess"] = file.read().decode()
                    modified = True
            elif file.name == "process-compute.config":
                config["compute"] = file.read().decode()
                modified = True
            else:
                if file.name.endswith(".json"):
                    key = file.name[len("process-"):-(len(".json"))]
                    if key in config:
                        config[key] = json.load(file)
                        modified = True

        if modified:
            st.session_state["config"] = config
            # Redraw the form
            self.reset()


def configure_workflow_app():
    """Launch an interactive interface for configuring a workflow."""

    # Create a configuration object, loading any files that are already present
    config = WorkflowConfig()

    # Launch an interactive display allowing the user to modify
    # the workflow configuration
    config.serve()


if __name__ == "__main__":
    configure_workflow_app()
