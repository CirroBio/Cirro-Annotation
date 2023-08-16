#!/usr/bin/env python3

import json
import re
from typing import List, Optional
from cirro import DataPortal
from cirro.sdk.process import DataPortalProcess
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.project import DataPortalProject
import pandas as pd
from pandas.errors import ParserError
from pathlib import Path
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
        # Set up an empty table of column names
        self.column_data = []
        for process in self.list_processes():
            if process.name not in [
                # "Gene Expression (nf-core/rnaseq)",
                "MAGeCK Count",
                "MAGeCK Flute",
                "Immune Clonotypes",
                "Differential Expression",
                "Gene Set Enrichment Analysis",
                "Lymphocyte Quantification"
            ]:  # FIXME
                continue
            # Add to the column_data list
            self.infer_process(process)

        # Analyze the aggregated column_data
        self.parse_terms()

    def parse_terms(self) -> None:
        """
        Based on the collection of column names identified so far,
        make/update a dictionary of terms.
        """

        # Load any terms which already exist
        if Path("terms.json").exists():
            self.terms = json.load(open("terms.json"))
        else:
            self.terms = {}

        # Concatenate the list of terms
        if isinstance(self.column_data, list):
            self.column_data = (
                pd.concat(self.column_data)
                .reset_index(drop=True)
            )

        # Compute a sanitized column name
        self.column_data = self.column_data.assign(
            sanitized_cname=self.column_data['cname'].apply(
                self.sanitize_cname
            )
        )

        # Get the terminal file name
        self.column_data = self.column_data.assign(
            filename = self.column_data["file"].apply(
                lambda s: s.split("/")[-1]
            )
        )

        # For each of the sanitized names
        for sani, df in self.column_data.groupby("sanitized_cname"):

            # If the entry doesn't already exist, add it
            if sani not in self.terms:
                self.terms[sani] = {
                    "column": [],
                    "metadata": [
                        {
                            "process": "*",
                            "file": "*",
                            "name": sani,
                            "desc": ""
                        }
                    ]
                }

            # For each of the observed names
            for cname in df["cname"].unique():
                # Make sure it is listed
                if cname not in self.terms[sani]["column"]:
                    self.terms[sani]["column"].append(cname)

            # For each of the files which includes this column name
            for _, r in df.iterrows():
                already_present = False
                for metadata in self.terms[sani]["metadata"]:
                    if already_present:
                        break
                    if metadata["process"] == r["process_name"]:
                        if metadata["file"].split("/")[-1] == r["filename"]:
                            already_present = True
                # If it's not already present, add it
                if not already_present:
                    self.terms[sani]["metadata"].append(
                        {
                            "process": r["process_name"],
                            "file": r["file"],
                            "name": "",
                            "desc": ""
                        }
                    )

        # Write out the terms JSON
        with open("terms.json", "wt") as handle:
            json.dump(self.terms, handle, indent=4)
        st.write("Wrote out updated terms JSON")

    @st.cache_data
    def sanitize_cname(_self, cname: str):
        cname = re.sub('[^0-9a-zA-Z]+', '_', cname.lower().strip()).strip("_")
        while "__" in cname:
            cname = cname.replace("__", "_")
        return cname

    def infer_process(self, process: DataPortalProcess) -> None:
        """Infer output file format based on available datasets."""

        # Gather the files from all available datasets
        files = self.list_files_from_process(process.id)
        if files.shape[0] == 0:
            return

        # Get the columns from each file
        columns = self.get_columns_from_files(files)
        self.column_data.append(columns)

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
                cname=cname,
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
        # _self.write_status(f"Reading {file} from {project_id} - {dataset_id}")
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
                dataset_name=dataset.name,
                process_id=process_id,
                process_name=_self.get_process_name(process_id)
            )
            for project in [_self.portal.get_project_by_name("Data Core Development")]
            for dataset in _self.list_datasets(project.id)
            if dataset.process_id == process_id
            for file in dataset.list_files()
            if _self.valid_dsv_extension(file.name)
        ])

    @st.cache_data
    def get_process_name(_self, process_id):
        return _self.portal.get_process_by_id(process_id).name

    def valid_dsv_extension(self, fn):
        if fn.endswith(".gz"):
            fn = fn[:-len(".gz")]
        return fn.endswith((".csv", ".tsv"))
        # return fn.endswith((".csv", ".tsv", ".txt"))


if __name__ == "__main__":
    infer_outputs = InferOutputs()
    infer_outputs.run()
