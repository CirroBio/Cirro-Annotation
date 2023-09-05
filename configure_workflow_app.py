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
def list_processes(ingest=False) -> List[str]:

    # Connect to Cirro
    portal: DataPortal = st.session_state['DataPortal']

    # List the projects available
    process_list = portal.list_processes(ingest=ingest)
    if ingest:
        process_list = process_list + portal.list_processes()

    # Return the list of processes available
    # (using their easily-readable names)
    process_list = list(set([
        f"{process.name} ({process.id})"
        for process in process_list
    ]))
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

    # print(f"Reading {fn} from {project_name} / {dataset_name}")
    return (
        get_dataset(project_name, dataset_name)
        .list_files()
        .get_by_name(f"data/{fn}")
        .read_csv(**kwargs)
    )


class WorkflowConfigElement:
    """Parent class for workflow configuration elements."""

    workflow_config: 'WorkflowConfig'

    def __init__(self, workflow_config: 'WorkflowConfig'):
        self.workflow_config = workflow_config

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

    def __init__(self, workflow_config: 'WorkflowConfig'):
        self.workflow_config = workflow_config
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

        # Get the updated value
        key = f"{self._id}.{kw}.{st.session_state.get('form_ix', 0)}"
        val = st.session_state[key]

        # If no change has been made
        if self.__dict__[kw] == val:
            # Take no action
            return

        # Otherwise, make the change and redraw the form
        self.__dict__[kw] = val
        config.save_config()
        config.reset()

    def input_kwargs(self, config: 'WorkflowConfig', kw: str):
        return dict(
            key=f"{self._id}.{kw}.{st.session_state.get('form_ix', 0)}",
            on_change=self.update_value,
            args=(config, kw)
        )

    def input_process_kwargs(self, config: 'WorkflowConfig', kw: str):
        return dict(
            key=f"{self._id}.{kw}.{st.session_state.get('form_ix', 0)}",
            on_change=self.update_process_list,
            args=(config, kw)
        )

    def get_process_id(self, long_name: str):
        return long_name.rsplit(" (", 1)[-1].rstrip(")")

    def update_process_list(self, config: 'WorkflowConfig', kw: str):
        key = f"{self._id}.{kw}.{st.session_state.get('form_ix', 0)}"
        process_list = st.session_state[key]

        # Get the process IDs for each process
        process_list = [
            self.get_process_id(process)
            for process in process_list
        ]

        if self.__dict__[kw] == process_list:
            return

        self.__dict__[kw] = process_list
        config.save_config()
        config.reset()

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """

        config.form_container.text_input(
            "Workflow ID",
            self.id,
            help="Must be all lowercase alphanumeric with dashes",
            **self.input_kwargs(config, "id")
        )

        config.form_container.text_input(
            "Workflow Name",
            value=self.name,
            help="Short name used to display the workflow in a list",
            **self.input_kwargs(config, "name")
        )

        config.form_container.text_input(
            "Workflow Description",
            value=self.desc,
            help="Longer description providing more details on the workflow (8-15 words)",
            **self.input_kwargs(config, "desc")
        )

        config.form_container.radio(
            "Workflow Executor",
            ["Nextflow", "Cromwell"],
            ["Nextflow", "Cromwell"].index(self.executor.title()),
            **self.input_kwargs(config, "executor")
        ).upper()

        config.form_container.text_input(
            "Workflow Repository (GitHub)",
            help="For private workflows, make sure to [install the CirroBio app](https://github.com/apps/cirro-data-portal) to provide access",
            value=self.uri,
            **self.input_kwargs(config, "uri")
        )

        config.form_container.text_input(
            "Workflow Entrypoint",
            value=self.script,
            help="Script from the repository used to launch the workflow",
            **self.input_kwargs(config, "script")
        )

        config.form_container.text_input(
            "Repository Version",
            value=self.version,
            help="Supports branch names, commits, tags, and releases.",
            **self.input_kwargs(config, "version")
        )

        config.form_container.selectbox(
            "Public / Private",
            ["GITHUBPUBLIC", "GITHUBPRIVATE"],
            ["GITHUBPUBLIC", "GITHUBPRIVATE"].index(self.repository),
            help="Supports branch names, commits, tags, and releases.",
            **self.input_kwargs(config, "repository")
        )

        config.form_container.multiselect(
            "Parent Processes",
            list_processes(ingest=True),
            [
                process for process in list_processes(ingest=True)
                if self.get_process_id(process) in self.parentProcessIds
            ],
            help="Datasets produced by parent processes can be used as inputs to run this workflow",
            **self.input_process_kwargs(config, "parentProcessIds")
        )

        config.form_container.multiselect(
            "Child Processes",
            list_processes(),
            [
                process for process in list_processes()
                if self.get_process_id(process) in self.childProcessIds
            ],
            help="Child processes can be run on the datasets produced as outputs by this workflow",
            **self.input_process_kwargs(config, "childProcessIds")
        )


class Param:

    workflow_config: 'WorkflowConfig'
    input_type: str
    input_type_options = [
        "Form Entry",
        "Output Directory",
        "Input Directory",
        "Hardcoded Value",
        "Dataset Name"
    ]

    input_type_values = {
        "Output Directory": "$.params.dataset.s3|/data/",
        "Input Directory": "$.params.inputs[0].s3",
        "Dataset Name": "$.params.dataset.name"
    }

    form_value_types = [
        "integer",
        "number",
        "string",
        "boolean",
        "array"
    ]
    deleted = False

    def __init__(
        self,
        kw: str,
        param_config: dict,
        workflow_config: 'WorkflowConfig'
    ):

        self.id: str = kw
        self.value: str = param_config["input"][kw]
        self.workflow_config = workflow_config

        # If the value is one of the hardcoded cases
        if self.value in self.input_type_values.values():
            self.input_type = {
                v: k for k, v in self.input_type_values.items()
            }[self.value]

        # If the value references a form element
        elif self.value.startswith("$.params.dataset.paramJson."):
            self.input_type = "Form Entry"

            # Find the location of the form which is referenced
            self.form_key = self.value[
                len("$.params.dataset.paramJson."):
            ].split(".")

            # Save the form elements of this param and all of its parents
            self.form_elements = {
                '.'.join(
                    self.form_key[:(i + 1)]
                ): self.get_form_element(
                    param_config,
                    self.form_key[:(i + 1)]
                )
                for i in range(len(self.form_key))
            }

        # Fallback - hardcoded value
        else:
            self.input_type = "Hardcoded Value"

    @property
    def form_config(self) -> dict:
        return self.form_elements[".".join(self.form_key)]

    def get_form_element(self, param_config: dict, path: str):
        form = param_config["form"]["form"]
        # Iterate over the keys in the path
        for ix, kw in enumerate(path):

            # If the keyword is not in the form for some reason
            if kw not in form["properties"]:

                # If it is the terminal keyword
                if len(path) == len(self.form_key) and ix == len(path) - 1:

                    # Set it up as a simple string
                    form["properties"][kw] = dict(
                        type="string",
                        default=kw,
                        title=kw
                    )

                # If it is an internal node
                else:

                    # Set it up as a simple object
                    form["properties"][kw] = dict(
                        properties=dict(),
                        type="object"
                    )

            form = form["properties"][kw]

        return {
            kw: val
            for kw, val in form.items()
            if kw != "properties"
        }

    def dump(self, workflow_config: dict):

        if self.deleted:
            return

        # Populate the form element, along with all parent levels
        if self.input_type == "Form Entry":

            # All new params will exist at the root level
            if "form_key" not in self.__dict__:
                self.form_key = [self.id]

            if "form_elements" not in self.__dict__:
                self.form_elements = {
                    '.'.join(self.form_key): {}
                }

            # Set up a pointer for navigating the form
            pointer = workflow_config["form"]["form"]

            for i in range(len(self.form_key)):
                if "properties" not in pointer:
                    pointer["properties"] = dict()

                val = self.form_elements['.'.join(self.form_key[:(i + 1)])]

                if self.form_key[i] not in pointer["properties"]:
                    pointer["properties"][self.form_key[i]] = val

                pointer = pointer["properties"][self.form_key[i]]

        # Populate the special-case hardcoded values
        elif self.input_type in self.input_type_values:
            self.value = self.input_type_values[self.input_type]

        # In all cases, add the value to the input spec
        workflow_config["input"][self.id] = self.value

    def serve(self, config: 'WorkflowConfig'):
        # Set up an expander for this parameter
        self.expander = config.params_container.expander(
            f"Input Parameter: '{self.id}'",
            expanded=True
        )

        # Set up a drop-down for the input type
        self.dropdown(
            "input_type",
            "Input Type",
            self.input_type_options,
            index=self.input_type_options.index(self.input_type)
        )

        if self.input_type == "Form Entry":
            self.dropdown(
                "form.type",
                "Form Value Type",
                self.form_value_types,
                self.form_value_types.index(self.form_config["type"])
            )

            if self.form_config["type"] == "string":

                self.text_input(
                    "form.default",
                    "Default Value",
                    self.form_config.get('default', ""),
                )

            elif self.form_config["type"] == "number":

                self.number_input(
                    "form.default",
                    "Default Value",
                    self.form_config.get('default', ""),
                )

            elif self.form_config["type"] == "integer":

                self.integer_input(
                    "form.default",
                    "Default Value",
                    self.form_config.get('default', ""),
                )

            elif self.form_config["type"] == "boolean":

                if "value" not in self.form_config:
                    self.form_config["value"] = False
                elif not isinstance(self.form_config["value"], bool):
                    self.form_config["value"] = False

                self.dropdown(
                    "form.default",
                    "Default Value",
                    [True, False],
                    [True, False].index(self.form_config["value"])
                )

        elif self.input_type == "Hardcoded Value":
            self.text_input(
                "value",
                "Value",
                self.value,
            )

        # Add a button to remove the parameter
        self.expander.button(
            "Remove",
            key=self.ui_key("_remove"),
            on_click=self.remove
        )

    def remove(self):
        """Remove this param from the inputs."""

        self.deleted = True
        self.workflow_config.save_config()
        self.workflow_config.reset()

    def text_input(self, kw, title, value):

        self.expander.text_input(
            title,
            value,
            **self.input_kwargs(kw)
        )

    def number_input(self, kw, title, value):

        self.expander.number_input(
            title,
            value,
            **self.input_kwargs(kw)
        )

    def integer_input(self, kw, title, value):

        try:
            value = int(value)
        except ValueError:
            value = 0

        self.expander.number_input(
            title,
            value,
            step=1,
            **self.input_kwargs(kw)
        )

    def dropdown(self, kw, title, options, index):

        self.expander.selectbox(
            title,
            options,
            index=index,
            **self.input_kwargs(kw)
        )

    def input_kwargs(self, kw):
        return dict(
            key=self.ui_key(kw),
            on_change=self.update_attribute,
            args=(kw,)
        )

    def ui_key(self, kw: str):
        return f"params.{self.id}.{kw}.{st.session_state.get('form_ix', 0)}"

    def update_attribute(self, kw: str):
        val = st.session_state[self.ui_key(kw)]
        # If we are changing a form element
        if kw.startswith("form."):

            # If the value is the same
            if val == self.form_elements[
                ".".join(self.form_key)
            ][
                kw[len("form."):]
            ]:
                # Take no action
                return

            # If the value is different, update the form
            # and then redraw the form (below)
            self.form_elements[
                ".".join(self.form_key)
            ][
                kw[len("form."):]
            ] = val
        else:
            # If the value is the same
            if val == self.__dict__[kw]:
                # Take no action
                return
            # If the value is different, update the attribute
            # And then redraw the form (below)

            self.__dict__[kw] = val

            # If we are updating the parameter type
            if kw == "input_type":
                # If there is a hardcoded value
                if val in self.input_type_values:
                    self.value = self.input_type_values[val]
                else:
                    self.value = ""

        self.workflow_config.save_config()
        self.workflow_config.reset()


class ParamsConfig(WorkflowConfigElement):

    params: List[Param]

    def load(self, config: dict) -> None:
        """
        Set up attributes based on the contents
        of the configuration JSON
        """

        # Set up an empty list of params
        self.params = []

        # Load params based on their being listed in the form
        for kw in config["input"].keys():

            # Set up a param object for this keyword value
            self.params.append(
                Param(kw, config, self.workflow_config)
            )

    def dump(self, config: dict) -> None:
        """
        The attributes of the configuration will be
        populated based on the state of this element.
        """

        for param in self.params:
            param.dump(config)

    def serve(self, config: 'WorkflowConfig') -> None:
        """
        Serve the user interaction for modifying the element
        """
        for param in self.params:
            param.serve(config)


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
            SourceConfig(workflow_config=self),
            ParamsConfig(workflow_config=self),
            OutputsConfig(workflow_config=self),
            PreprocessConfig(workflow_config=self),
            ComputeConfig(workflow_config=self),
        ]

    def save_config(self) -> None:
        """Save a new copy of the config in the session state."""

        # Save the previous version
        self.save_history()

        # Update the session state
        st.session_state["config"] = self.format_config()

    def save_history(self):
        "Save the current config to history"

        if (
            st.session_state.get("config") is not None and
            st.session_state.get("history", [[]])[0] != st.session_state["config"]
        ):
            st.session_state[
                "history"
            ] = [
                st.session_state["config"]
            ] + st.session_state.get(
                "history", []
            )

    def format_config(self) -> dict:
        """Generate a config file based on the app state."""

        # Make a blank copy
        config = {
            kw: default
            for kw, default in [
                ("dynamo", dict()), 
                ("form", dict(form=dict(), ui=dict())), 
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
            "Analysis Workflow",
            "Input Parameters",
            "Output Files",
            "Cirro Configuration"
        ]
        self.tabs = dict(zip(tab_names, st.tabs(tab_names)))

        # Set up tabs for the configuration elements
        config_tabs = [
            "Dynamo",
            "Form",
            "Input",
            "Compute",
            "Preprocess",
            "Output"
        ]
        self.tabs = {
            **self.tabs,
            **dict(zip(
                config_tabs,
                self.tabs["Cirro Configuration"].tabs(
                    config_tabs
                )
            ))
        }

        # Set up an empty in each of the tabs
        self.tabs_empty: Dict[str, DeltaGenerator] = {
            kw: tab.empty()
            for kw, tab in self.tabs.items()
        }

        # Let the user upload files
        self.add_file_uploader()

        # Set up an empty which will be populated with "Download All" button
        self.download_all_empty = st.sidebar.empty()

        # Set up columns for the Undo and Redo buttons
        undo_col, redo_col = st.sidebar.columns(2)

        # Set up empty elements which will be populated with Undo/Redo
        self.undo_empty = undo_col.empty()
        self.redo_empty = redo_col.empty()

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

        # Set up the containers
        self.form_container = self.tabs_empty["Analysis Workflow"].container()
        self.params_container = self.tabs_empty["Input Parameters"].container()

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
                    key=f"download.{kw}.{st.session_state.get('form_ix', 0)}"
                )

                # Print the element in the tab
                cont.text(text)

        # Let the user download all files as a zip
        self.download_all_empty.download_button(
            "Download all (ZIP)",
            zip_buffer,
            file_name="cirro-configuration.zip",
            key=f"download.all.{st.session_state.get('form_ix', 0)}"
        )

        # If there is any history
        if len(st.session_state.get("history", [])) > 0:
            # Add the undo button
            self.undo_empty.button(
                "Undo",
                key=f"undo.{st.session_state.get('form_ix', 0)}",
                on_click=self.undo,
                use_container_width=True
            )
        else:
            # If no history is present, clear the button
            self.undo_empty.empty()

        # If there is any future
        if len(st.session_state.get("future", [])) > 0:
            # Add the redo button
            self.redo_empty.button(
                "Redo",
                key=f"redo.{st.session_state.get('form_ix', 0)}",
                on_click=self.redo,
                use_container_width=True
            )
        else:
            # If no future is present, clear the button
            self.redo_empty.empty()

    def undo(self):
        """Action performed by the Undo button."""

        # Put the current config in the future
        st.session_state["future"] = (
            [st.session_state["config"]] +
            st.session_state.get("future", [])
        )

        # Get the first config from the history
        old_config = st.session_state["history"].pop()

        # Update the current state
        st.session_state["config"] = old_config

        # Reset the display
        self.reset()

    def redo(self):
        """Action performed by the Redo button."""

        # Put the current config in the history
        st.session_state["history"] = (
            [st.session_state["config"]] +
            st.session_state.get("history", [])
        )

        # Get the first config from the future
        old_config = st.session_state["future"].pop()

        # Update the current state
        st.session_state["config"] = old_config

        # Reset the display
        self.reset()

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

            # Save the previous version
            self.save_history()

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
