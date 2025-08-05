# UFO & Drone Sightings Analysis

## Project Overview

This project explores UFO and drone sightings across the United States using open-source data. By merging reports of unexplained aerial phenomena with FAA drone encounter logs, we aim to uncover patterns in time, geography, and frequency. The project is designed for those interested in data analysis, public safety, or curious phenomena.

---

## Project Objective

**Can UFO and drone sightings be compared by location and time? Are there regional or seasonal patterns that suggest overlap or misidentification between them?**  
This analysis seeks to uncover trends and correlations between these sightings using data wrangling, visualization, and geospatial plotting.

---

## Technologies Used

- **Python** – Core language for data cleaning, analysis, and visualization  
- **Pandas** – For reading, cleaning, transforming, and merging datasets  
- **Matplotlib & Seaborn** – Used to create bar and line charts for comparison  
- **Plotly Express** – Used for interactive maps of sightings across geographic regions  
- **Jupyter Notebooks** – Used for documenting the project and presenting findings  
- **Git & GitHub** – Used for version control and project sharing

---

## Project Setup Instructions

### Prerequisites

Ensure you have the following installed:
- Python 3.8+
- pip
- Git
- VS Code or another IDE

### Setup Steps

1. **Clone the Repository**

```bash
git clone https://github.com/amydaniel24/ufo-analysis-capstone.git
cd ufo-analysis-capstone
python -m venv venv

# On Windows:
venv\Scripts\activate

# On macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt

jupyter notebook

###  Data Summary

The merged dataset consists of reports from both UFO sightings and FAA-reported drone encounters in 2022. After cleaning and combining the data:

- **Total Records**: 79,641  
- **UFO Sightings**: 77,821  
- **Drone Sightings**: 1,820  
- **Date Range**: January 1, 2022 — December 20, 2022  
- **Most Common State**: California (`ca`)  
- **Most Common City**: Seattle  
- **Total Columns**: 10  
- **Missing Values**: All missing values were removed from key columns before analysis

This dataset includes both:
- **Quantitative data** (e.g., date, month, latitude, longitude)
- **Qualitative data** (e.g., state, city, type, summary)

The data is well-suited for time series analysis, geographic plotting, and pattern comparison between reported UFO and drone sightings across the United States.

| Column Name          | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| `date_only`          | Date of the sighting report (formatted as YYYY-MM-DD)                  |
| `City`               | City where the sighting occurred                                       |
| `State`              | U.S. state abbreviation where the sighting was reported                |
| `type`               | Type of sighting: `"ufo"` or `"drone"`                                 |
| `Summary`            | Description or narrative of the reported sighting                      |
| `month`              | Numeric month extracted from the sighting date (1 = Jan, 12 = Dec)     |
| `latitude`           | Approximate latitude coordinate of the sighting location               |
| `longitude`          | Approximate longitude coordinate of the sighting location              |
| `nearest_park`       | Name of the closest U.S. National Park to the sighting                 |
| `population_density` | Estimated population density (people per square mile) for the location |


###  Data Summary

The merged dataset consists of reports from both UFO sightings and FAA-reported drone encounters in 2022. After cleaning and combining the data:

- **Total Records**: 79,641  
- **UFO Sightings**: 77,821  
- **Drone Sightings**: 1,820  
- **Date Range**: January 1, 2022 — December 20, 2022  
- **Most Common State**: California (`ca`)  
- **Most Common City**: Seattle  
- **Total Columns**: 10  
- **Missing Values**: All missing values were removed from key columns before analysis

This dataset includes both:
- **Quantitative data** (e.g., date, month, latitude, longitude)
- **Qualitative data** (e.g., state, city, type, summary)

The data is well-suited for time series analysis, geographic plotting, and pattern comparison between reported UFO and drone sightings across the United States.

###  Data Source

This project uses open-source data from two primary sources:

- **UFO Sightings Dataset**  
  Source: [National UFO Reporting Center](https://nuforc.org/data/)  
  Description: This dataset contains thousands of reports of UFO sightings across the United States, including date, time, location, and summary information.

- **Drone Sightings Dataset**  
  Source: [Federal Aviation Administration (FAA)](https://www.faa.gov/uas/resources/public_records/uas_sightings_report)  
  Description: This dataset includes preliminary reports from FAA on drone sightings by pilots and air traffic controllers, with dates, locations, and event summaries.

All data was downloaded in CSV format and cleaned using Python.

###  Version Control and GitHub Practices

This project follows professional version control practices:

- All work was tracked using **Git**, with **at least 10 commits** made throughout the development process.
- Commits were performed using the **command line interface (CLI)**, not the GitHub web uploader.
- Each commit includes a meaningful message to reflect changes made during that session.
- The complete project—including code, data, and documentation—is maintained in a **GitHub repository**:  
   [https://github.com/amydaniel24/ufo-analysis-capstone](https://github.com/amydaniel24/ufo-analysis-capstone)

  ###  Notebook Guidelines

This project is developed and presented in a **Jupyter Notebook**, following best practices for readability and clarity:

- **Markdown cells** explain the purpose, logic, and results of the code throughout the notebook.
- **Clean and organized structure**: Test code and scratch work have been removed.
- **Consistent formatting** is used for readability, including proper spacing, headers, and labels.
- **Visualizations** use clear titles, labeled axes, and cohesive color palettes to improve interpretability.
- All datasets are loaded using **relative file paths** (e.g., `./data/file.csv`) to ensure cross-system compatibility.

> The notebook is designed for users without a coding background to follow the full data analysis journey.

---

## ✅ Capstone Completion Checklist

| Requirement                                      | Status   |
|--------------------------------------------------|----------|
| Uses at least two open-source datasets           | ✅        |
| Each dataset has 1,000+ rows and 10+ columns     | ✅        |
| Includes both qualitative & quantitative data    | ✅        |
| Project folder is structured and readable        | ✅        |
| README includes project overview & setup steps   | ✅        |
| Notebook includes Markdown cells with analysis   | ✅        |
| Uses relative file paths                         | ✅        |
| Includes data dictionary                         | ✅        |
| Includes data summary                            | ✅        |
| Includes visualizations                          | ✅        |
| Shows findings from analysis                     | ✅        |
| GitHub repo has at least 10 commits              | ✅        |
| Final summary & conclusion in notebook           | ✅        |
| All code is clean, readable, and commented       | ✅        |

Project Complete ✅
