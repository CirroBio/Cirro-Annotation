# Recursive HDF5 to Parquet Converter

![Project Logo](logo.png) <!-- If you have a project logo, add it here -->

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The Recursive HDF5 to Parquet Converter is a Python-based tool that enables users to recursively navigate through HDF5 files and convert their contents into Parquet files. HDF5 (Hierarchical Data Format) files are commonly used to store large volumes of complex scientific data, while Parquet is a columnar storage file format that is highly optimized for analytics and big data processing.

This converter offers a convenient way to convert HDF5 files into Parquet format, which can significantly improve data analysis performance and storage efficiency, especially when working with large datasets.

## Features

- Recursively navigates through directories to find HDF5 files
- Converts HDF5 files into Parquet files
- Preserves the hierarchical structure of the original HDF5 files
- Efficiently handles large datasets
- Parallel processing for faster conversion (optional)
- Customizable options for conversion settings

## Installation

1. Ensure you have Python 3.x installed on your system.
2. Clone this GitHub repository:

   ```
   git clone https://github.com/your_username/recursive-hdf5-to-parquet.git
   cd recursive-hdf5-to-parquet
   ```

3. Install the required dependencies using pip:

   ```
   pip install -r requirements.txt
   ```

## Usage

The Recursive HDF5 to Parquet Converter can be used through the command line interface (CLI). The basic syntax is as follows:

```
python converter.py --input <input_directory> --output <output_directory>
```

Replace `<input_directory>` with the path to the root directory containing your HDF5 files and `<output_directory>` with the destination directory where the Parquet files will be stored.

### Optional Parameters

- `--num_threads`: Number of threads for parallel processing (default: maximum available cores)
- `--compression`: Specify the compression algorithm for Parquet files (default: 'snappy')
- `--verbose`: Enable verbose mode to display detailed logs (default: False)

## Examples

1. Convert HDF5 files in the current directory and save Parquet files in the 'output' directory:

   ```
   python converter.py --input . --output output
   ```

2. Convert HDF5 files in a specific directory with verbose logging and 4 threads:

   ```
   python converter.py --input /path/to/hdf5_files --output /path/to/parquet_files --verbose --num_threads 4
   ```

## Contributing

Contributions to the Recursive HDF5 to Parquet Converter are welcome! If you have any bug fixes, improvements, or new features to propose, please open an issue or submit a pull request. For major changes, it's best to discuss your ideas first with the project maintainers.

When contributing, please ensure that your code follows the existing coding style and includes appropriate tests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
