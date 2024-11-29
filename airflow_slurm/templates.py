from jinja2 import Template

SLURM_FILE = Template(
"""#!/bin/bash
{%- for opt, value in slurm_opts.items() %}
#SBATCH {{ value }}
{%- endfor %}

{%- if modules %}
# Load required modules
{%- for module in modules %}
module load {{ module }}
{%- endfor %}
{%- endif %}

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