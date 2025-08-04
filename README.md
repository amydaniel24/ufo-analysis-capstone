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
