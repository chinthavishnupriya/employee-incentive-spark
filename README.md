# Employee Incentive System using PySpark

A practical PySpark project that calculates employee sales for three months, determines incentive eligibility, and uses a Spark Broadcast Variable to distribute incentive configuration efficiently.

## Project Objective

An employee is eligible for an incentive when their total sales for three months are greater than or equal to ₹100,000. Eligible employees receive an incentive of ₹20,000.

## Input Data

### `data/sales.csv`

Contains:
- Employee ID
- Employee name
- Month 1 sales
- Month 2 sales
- Month 3 sales

### `data/incentive_config.csv`

Contains the business rules:
- Minimum sales: ₹100,000
- Incentive amount: ₹20,000

## Processing Flow

```text
sales.csv
   ↓
Spark DataFrame
   ↓
Calculate 3-month total sales
   ↓
Read incentive configuration
   ↓
Create Spark Broadcast Variable
   ↓
Apply eligibility rule
   ↓
Eligible / Not Eligible
   ↓
Write CSV outputs
```

## Broadcast Variable

The incentive configuration is small but is required while processing employee records. PySpark's Broadcast Variable distributes this read-only configuration to worker nodes efficiently instead of repeatedly sending the same configuration with tasks.

The project uses:

```python
broadcast_config = spark.sparkContext.broadcast(incentive_config)
```

Workers access the configuration using:

```python
broadcast_config.value
```

## Sample Result

| Employee | 3-Month Sales | Status | Incentive |
|---|---:|---|---:|
| Ravi | ₹105,000 | Eligible | ₹20,000 |
| Kiran | ₹75,000 | Not Eligible | ₹0 |
| Arjun | ₹105,000 | Eligible | ₹20,000 |
| Priya | ₹75,000 | Not Eligible | ₹0 |
| Suresh | ₹120,000 | Eligible | ₹20,000 |

### Summary

- Eligible employees: 3
- Not eligible employees: 2
- Total incentive paid: ₹60,000

## Project Structure

```text
employee-incentive-spark/
├── incentive.py
├── data/
│   ├── sales.csv
│   └── incentive_config.csv
├── output/              # Generated locally; ignored by Git
└── .gitignore
```

## Requirements

- Python 3.11
- PySpark 3.5.3
- Java 17

## How to Run

From the project directory:

```bash
python incentive.py
```

The program generates:

```text
output/eligible/
output/not_eligible/
```

## Technologies

- Python
- PySpark
- Spark DataFrames
- Spark RDD
- Spark Broadcast Variable
- CSV
- Git/GitHub
