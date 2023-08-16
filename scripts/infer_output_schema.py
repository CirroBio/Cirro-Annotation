#!/usr/bin/env python3

from typing import List, Optional
from cirro import DataPortal
from cirro.sdk.process import DataPortalProcess
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.project import DataPortalProject
import pandas as pd
from pandas.errors import ParserError
import streamlit as st


class InferOutputs:

    def __init__(self) -> None:
        if not st.session_state.__contains__("DataPortal"):
            st.session_state["DataPortal"] = DataPortal()

    @property
    def portal(self) -> DataPortal:
        return st.session_state["DataPortal"]

    @st.cache_resource
    def list_processes(_self) -> List[DataPortalProcess]:
        return _self.portal.list_processes()

    @st.cache_resource
    def list_projects(_self) -> List[DataPortalProject]:
        return _self.portal.list_projects()

    @st.cache_resource
    def list_datasets(_self, project_id) -> List[DataPortalDataset]:
        project = _self.portal.get_project_by_id(project_id)
        return project.list_datasets()

    def run(self) -> None:
        self.status = st.empty()
        for process in self.list_processes():
            if process.name != "Gene Expression (nf-core/rnaseq)":  # FIXME
                continue
            self.infer_process(process)

    def infer_process(self, process: DataPortalProcess) -> None:
        """Infer output file format based on available datasets."""

        # Gather the files from all available datasets
        files = self.list_files_from_process(process.id)
        st.write(files)

        # Get the columns from each file
        columns = self.get_columns_from_files(files)

    def get_columns_from_files(self, files: pd.DataFrame) -> pd.DataFrame:

        return pd.DataFrame(
            column
            for _, r in files.iterrows()
            for column in self.get_columns_from_single_file(**r.to_dict())
        )

    def get_columns_from_single_file(
        self,
        file=None,
        project_id=None,
        dataset_id=None,
        **kwargs
    ):

        # Read the first few lines of the file
        sep = "," if "csv" in file else "\t"
        head = self.read_csv(
            project_id,
            dataset_id,
            file,
            sep=sep,
            nrows=10
        )
        return [
            dict(
                project_id=project_id,
                dataset_id=dataset_id,
                file=file,
                **kwargs
            )
            for cname in head.columns.values
        ]
    
    def write_status(self, msg):
        self.status.write(msg)

    @st.cache_resource
    def read_csv(
        _self,
        project_id,
        dataset_id,
        file,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        _self.write_status(f"Reading {file} from {project_id} - {dataset_id}")
        try:
            return (
                _self
                .portal
                .get_project_by_id(project_id)
                .get_dataset_by_id(dataset_id)
                .list_files()
                .get_by_name(file)
                .read_csv(**kwargs)
            )
        except ParserError:
            return pd.DataFrame()

    @st.cache_resource
    def list_files_from_process(_self, process_id):
        """Return a list of all of the files from all datasets of this type."""
        # _self.write_status(f"Finding files from {process_id} datasets")
        return pd.DataFrame([
            dict(
                file=file.name,
                project_id=project.id,
                dataset_id=dataset.id,
                project_name=project.name,
                dataset_name=dataset.name
            )
            for project in [_self.portal.get_project_by_name("Data Core Development")]
            for dataset in _self.list_datasets(project.id)
            if dataset.process_id == process_id
            for file in dataset.list_files()
            if _self.valid_dsv_extension(file.name)
        ])

    def valid_dsv_extension(self, fn):
        if fn.endswith(".gz"):
            fn = fn[:-len(".gz")]
        return fn.endswith((".csv",".tsv",".txt"))


if __name__ == "__main__":
    infer_outputs = InferOutputs()
    infer_outputs.run()
