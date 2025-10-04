# WorldBank Dashboard (Streamlit + MySQL)

Dashboard para explorar indicadores económicos (World Bank) conectando a una base MySQL local.

## Requisitos
- Python 3.10+
- MySQL con la base `worldbank` y tablas:
  - `observations(country_iso3, indicator_code, year, value)`
  - `indicators(indicator_code, indicator_name, unit)`
  - `countries(iso3, name)`

## Instalación
Clonar el proyecto y crear entorno virtual:
```bash
git clone https://github.com/<tu_usuario>/worldbank-dashboard.git
cd worldbank-dashboard

python -m venv .venv
# Windows:
.\.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

pip install -r requirements.txt
