# WorldBank Dashboard (Streamlit + MySQL)

Dashboard para explorar indicadores económicos (World Bank) conectando a una base MySQL local.

El origen de los datos: https://api.worldbank.org/v2

Los scripts de la base de datos MySQL
https://drive.google.com/drive/folders/1PSWyJtptMSG3Y-7vGxiiebu1-1NJcetr?usp=sharing


## Requisitos
- Python 3.10+
- MySQL con la base `worldbank` y tablas:
  - `observations(country_iso3, indicator_code, year, value)`
  - `indicators(indicator_code, indicator_name, unit)`
  - `countries(iso3, name)`

## Instalación
Clonar el proyecto y crear entorno virtual:
git clone https://github.com/cpereyra71/TP-segundo-modulo.git
cd TP-segundo-modulo
pip install -r requirements.txt
streamlit run TP_Streamlit_WorldBank.ipynb

