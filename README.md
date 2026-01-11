# Mini RDBMS – Pesapal Junior Dev Challenge 2026

This project is a **minimal relational database management system (RDBMS)** built from scratch in **Python** for the **Pesapal Junior Developer Challenge 2026**.

The goal of this project is to demonstrate **clear system design, problem-solving ability, and understanding of core database concepts**, rather than relying on existing database engines.

---

## Features

- SQL-like interface with an interactive REPL
- Table creation with schema enforcement
- Supported data types:
  - `INT`
  - `TEXT`
  - `BOOL`
- PRIMARY KEY and UNIQUE constraints
- Basic indexing using in-memory hash indexes
- Persistent storage using JSON files
- Full CRUD support:
  - `CREATE TABLE`
  - `INSERT`
  - `SELECT` with `WHERE`
  - `UPDATE`
  - `DELETE`
- Automated tests using `pytest`
- Minimal Flask web app demonstrating CRUD using the custom RDBMS

---

## Project Structure

pesapal-rdbms/
│
├── rdbms/ # Core database engine
│ ├── types.py # Data types and coercion
│ ├── catalog.py # Table schemas (catalog)
│ ├── storage.py # Disk persistence
│ ├── index.py # Hash-based indexes
│ ├── table.py # Database engine (CRUD logic)
│ ├── parser.py # SQL-like parser
│ ├── executor.py # Executes parsed statements
│ └── repl.py # Interactive REPL
│
├── tests/ # Automated tests
├── examples/ # Example SQL scripts
├── webapp/ # Minimal demo web app (CRUD)
├── data/ # Database files (generated at runtime)
│
├── README.md
├── pytest.ini
└── .gitignore


---

## Requirements

- Python **3.11+**
- No external database libraries are used

---

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/Aemarah29/pesapal-rdbms.git
cd pesapal-rdbms

