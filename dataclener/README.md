# datacleanser

A tiny Python utility to normalize 2D data rows (e.g., from pandas or numpy) for serialization, export, or processing.

## Features

- Handles `numpy.int64`, `numpy.float64`, `datetime`, `date`, `NaN`, `None`
- Converts everything else to `str`

## Installation

```bash
pip install datacleanser
