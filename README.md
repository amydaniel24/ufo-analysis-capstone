# UFO and Drone Sightings Analysis Capstone

## Overview

This project explores the patterns  between UFO sightings and drone activity across the United States. By combining publicly available datasets on UFOs, drone incidents, national parks, and population density, I looked for potential overlaps in timing, geography, and reporting trends.

## Project Objective

To analyze whether there is a correlation between UFO sightings and drone sightings in the U.S., and to examine how factors like location (proximity to national parks) and time (by year or month) might influence reporting.

## Table of Contents

1. Overview
2. Project Objective
3. Datasets
4. Setup Instructions
5. Usage
6. Project Structure
7. Project Features
8. Final Visualizations
9. Future Work

## Datasets

- **UFO Sightings Dataset:** [Source](https://www.kaggle.com/datasets/NUFORC/ufo-sightings)
- **Drone Sightings Dataset:** FAA reports obtained from public sources
- **National Parks Locations:** U.S. National Park Service data
- **Population Density by State:** Open-source data compiled from census.gov

## Setup Instructions

1. Clone this repository:
   ```bash
   git clone https://github.com/amydaniel24/ufo-analysis-capstone.git
   cd ufo-analysis-capstone
   ```

2. Open the project in VS Code

3. (Optional) Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # macOS/Linux
   .\venv\Scripts\activate  # Windows
   ```

4. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Run the notebook:
   Open `notebooks/final_capstone_notebook.ipynb` and run all cells.

## Project Structure

```
ufo-analysis-capstone/
│
├── data/                     # Raw and cleaned datasets
├── notebooks/                # Jupyter notebooks
│   └── final_capstone_notebook.ipynb
├── README.md                 # Project overview and instructions
├── requirements.txt          # Python dependencies
├── visuals/                  # Saved plots and graphs
└── output/                   # HTML version of final notebook
```

## Features

- Cleaned and merged data from multiple sources
- Analysis of sighting frequencies by year and state
- notebooks\final_capstone_notebook.ipynb
- Visuals with Plotly and Mapbox
- Spatial comparison near National Parks
- Comparison of reporting trends between UFO and drone sightings

## Final Visuals

Key visuals are available in the final notebook and the HTML export in the `output/` directory.

## Future Work

- Improve location matching accuracy for drone sightings
- Build classification model to predict likelihood of misclassification
- Incorporate FAA incident severity or description fields into analysis