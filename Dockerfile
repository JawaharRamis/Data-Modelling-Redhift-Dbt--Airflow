FROM quay.io/astronomer/astro-runtime:10.0.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-redshift && deactivate