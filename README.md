Banks ETL Pipeline

ETL pipeline que extrae datos de bancos globales desde un CSV,
aplica transformaciones con Pandas y carga los resultados 
en SQLite para análisis con SQL.

🛠️ Tecnologías
- Python 3
- Pandas
- SQLite3

📁 Estructura del proyecto
```
banks-etl-pipeline/
│
├── data/
│   └── banks.csv
├── etl_v2.py
├── requirements.txt
└── README.md
```

⚙️ Cómo ejecutarlo

1. Clona el repositorio
git clone https://github.com/Maicol-Porras/banks-etl-pipeline.git

2. Instala las dependencias
pip install -r requirements.txt

3. Ejecuta el pipeline
python etl_v2.py

🔄 Qué hace el pipeline

- **Extract:** Lee el archivo `banks.csv`
- **Transform:**
  - Ordena bancos por Market Cap (mayor a menor)
  - Filtra bancos con Market Cap > 150B USD
  - Agrupa y calcula promedios por país
- **Load:**
  - Guarda resultados en CSV
  - Carga datos en base de datos SQLite
  - Ejecuta consultas SQL de análisis

📊 Consultas SQL incluidas
- Todos los bancos
- Filtro por Market Cap > 150B
- Ranking por Market Cap
- Conteo de bancos por país

👨‍💻 Autor
Maicol Porras
[GitHub](https://github.com/Maicol-Porras)
