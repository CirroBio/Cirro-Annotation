# Cirro's Scientific Pipeline Annotation Tool

## Overview

Cirro's Annotation Tool processes the outputs from scientific pipelines and allows users to provide feedback. This feedback is integrated into a manifest JSON file, which subsequently can be utilized by other systems to produce web-optimized visualization assets. This CLI-based Python tool serves as an essential bridge between raw scientific data and interactive, user-friendly visual representations on the web.

## Features

- **Annotation Loop**: Efficiently loops through pipeline outputs and provides an interface for user feedback.
- **Manifest Creation**: Automatically synthesizes feedback and data into a structured manifest JSON file.
- **Web Visualization Compatibility**: Ensures that the produced manifest is optimized for web visualization tools.

## Getting Started

### Prerequisites

- Python 3.7 or newer
- Required libraries (see `requirements.txt` for details)

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/cirro-annotation-tool.git
    ```

2. Navigate to the project directory:
    ```bash
    cd cirro-annotation-tool
    ```

3. Install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```

### Usage

1. Run the CLI tool:
    ```bash
    python annotate_cli.py --input /path/to/pipeline/outputs/
    ```

2. Follow the on-screen prompts to annotate each output.

3. Upon completion, the tool will produce a `manifest.json` file in the current directory, ready for the web visualization process.

## How the Annotation Works
1. User selects a Cirro PROJECT that contains a dataset that should be annotated.
2. User selects the Cirro PROCESS that they want to annotate.
3. User selects an exemplar DATASET that is used to determine subsequent prompts and what files to annotate.
4. Script downloads dataset FILES that could be used for visualization
5. User selects all FILES that vary by run. (Files or file paths that contain dataset specific strings.)
6. User replaces portions of FILES or file paths that vary with [TOKENS] until all files are resolved.  The [TOKEN] must be all uppercase and wrapped in square brackets.  The copy in the brackets will ultimately be use for a column name and all files that match will be concatinated.
7. User selects all COLUMNS that vary by run. (Column names that contain datset specific strings.)
8. User sub-selects from the COLUMNS all columns that relate to a particular VARIABLE, until all columns are resolved.  
9. User provides a name and description for each VARIABLE.
