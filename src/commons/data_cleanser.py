import polars as pl
import polars.selectors as cs
from datetime import datetime
from pathlib import Path


def clean_data(df:pl.LazyFrame, filename:str, primary_key:str, target_columns:list[str]=None)->pl.DataFrame:

    # --- STEP 1: NAMING CONVENSION ---
    # Usamos collect_schema().names() para obtener las columnas iniciales sin alertas de rendimiento
    initial_cols = df.collect_schema().names()
    
    df_cleaned = (
        df
            .rename({col: col.strip().lower().replace(" ", "_") for col in initial_cols})
    )

    # OBTENEMOS EL SCHEMA INTERMEDIO EN MODO LAZY (Resuelve tus reglas de negocio)
    schema_post_rename = df_cleaned.collect_schema()

       # OBTENEMOS EL SCHEMA INTERMEDIO EN MODO LAZY
    schema_post_rename = df_cleaned.collect_schema()

    # --- CORRECCIÓN AQUÍ: Inicializar SIEMPRE la lista al principio ---
    casting_expressions = []

    # REGLA 1 y 2: Validamos si target_columns tiene elementos o no es null
    cleaned_targets = [
        clean for col in (target_columns or [])
        if (clean := col.strip().lower().replace(" ", "_")) in schema_post_rename
    ]
    subset_schema = df_cleaned.select(cleaned_targets).collect_schema() if cleaned_targets else {}

    # REGLA 3: Construcción dinámica si hay elementos en el schema
    for name, datatype in subset_schema.items():
        if datatype == pl.Float32:
            casting_expressions.append(pl.col(name).fill_null(pl.col(name).median()).cast(pl.Float32))
        elif datatype == pl.Float64:
            casting_expressions.append(pl.col(name).fill_null(pl.col(name).median()).cast(pl.Float64))
        elif datatype.is_integer():
            casting_expressions.append(pl.col(name).fill_null(pl.col(name).median()).cast(datatype))
            
    # --- PIPELINE DE LIMPIEZA ACTIVO (Modo Diferido / Lazy) ---
    filtered_df = (
        df_cleaned
            # --- STEP 2: DROP EMPTY OR NULL ROWS ---
            # Filtro optimizado para remover nulos en la llave primaria de forma segura
            .filter(pl.col(primary_key).is_not_null())
        
            # Inyección de tus expresiones de mediana personalizadas por tipo de dato
            .with_columns(casting_expressions)

            # --- STEP 3: TEXT HARMONIZATION ---
            # Solo aplica el reemplazo de texto a columnas que el schema detecta como String (Evita romper fechas)
            .with_columns([
                pl.when(
                    pl.col(col).is_null() | (pl.col(col).str.strip_chars() == "")
                )
                .then(pl.lit("UNKNOWN"))
                .otherwise(pl.col(col).str.strip_chars())
                .alias(col)
                for col in schema_post_rename.names()
                if schema_post_rename[col] == pl.String
            ])
   
             # --- PASO 4: DEDUPLICACIÓN ---
            .unique(maintain_order=True)
    )

    #4 Write Cleansed dataframe to parquet file on silver.
    parquet_dir = Path.cwd() / "data/3_cleaned" if (Path.cwd() / "data").exists() else Path.cwd().parent / "data/3_cleaned"
    parquet_path = parquet_dir / f"{filename}.parquet"

    filtered_df.sink_parquet(parquet_path)

    # 5. Retornamos el LazyFrame completamente preparado
    return filtered_df.collect(streaming=True)