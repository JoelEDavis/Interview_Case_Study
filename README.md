# Sprints Technical Case Study

This project implements an ETL (Extract, Transform, Load) process using FastAPI for the Sprints Case Study.

## Prerequisites

- Python 3.9 or higher
- Git (for cloning the repository)

## Installation

1. Clone the repository
   ```bash
   git clone https://github.com/JoelEDavis/Interview_Case_Study.git
   cd interview_case_study
   ```

2. Create a virtual environment
   ```bash
   python -m venv venv
   ```

3. Activate the virtual environment
   - Windows:
     ```bash
     venv\Scripts\activate
     ```
   - macOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. Install the project and its dependencies
   ```bash
   pip install -e .
   ```

## Project Structure

```
interview_case_study/
├── utils/                # Utility functions for ETL processing
├── pyproject.toml        # Project configuration and dependencies
├── README.md             # This file
└── main.py               # Main application entry point
```

## API Endpoints

Endpoints are available at https://technical-case-platform-engineer.onrender.com/docs

- `GET /`: Root endpoint that returns a welcome message
- `GET /docs`: Swagger UI documentation

## ETL Process

This project implements an ETL pipeline that:

1. **Extracts** monthly portfolio company and exchange rate data from the database via FastAPI endpoints
2. **Transforms** the data with pandas, a Python data analysis library
3. **Loads** the aggregated annual data to the database 

## Author

Joel Davis - jdavis2011@hotmail.co.uk