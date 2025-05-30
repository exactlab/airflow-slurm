from jinja2 import Template

SLURM_FILE = Template(
    """#!/bin/bash
{%- for opt, value in slurm_opts.items() %}
#SBATCH {{ value }}
{%- endfor %}

source ${HOME}/env

{%- if setup_commands %}
# Additional setup commands
{%- for cmd in setup_commands %}
{{ cmd }}
{%- endfor %}
{%- endif %}

# Job commands
{{ job_command }}
"""
)