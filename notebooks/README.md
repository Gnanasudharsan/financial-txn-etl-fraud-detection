# Notebooks

Exploration and demos only. The source of truth is `src/fintxn/`; notebooks import the package
rather than re-implementing logic.

- `explore_lakehouse.py` - Databricks/Jupytext-style notebook that reads the medallion tables
  through `fintxn.storage.Lake`, inspects quarantine reasons, and time-travels a silver table.
