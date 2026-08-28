import polars as pl


def clean_data(df:DataFrame, file_name:str, primary_key:str, colums:list[str])->pl.DataFrame:
     # 1 Execution Pipeline
    
     # 2. Pipeline cleansing data
     cleaned_df = (
        df
            # --- STEP 1: NAMING CONVENSION ---
            .rename({col: col.strip().lower().replace(" ", "_") for col in df.columns})
    
            # --- STEP 2: DROP EMPTY OR NULL ROWS ---
            # If the ID of the df is null, or is empty, it dropss the complete row
            .filter(
                pl.col(primary_key).is_not_null() & 
                (pl.col(primary_key).str.strip_chars() != "")
            )
            .with_columns([
                pl.col("voltage_kv").fill_null(pl.col("voltage_kv").median()).cast(target_dtype)
            ])
    
            # --- STEP 3: ADD INGESTION META DATA ---
            .with_columns([
                pl.lit(datetime.now()).alias("ingested_at"), 
                pl.lit(file_name).alias("ingested_from_file"),
            ])
    
            # --- STEP 4: TEXT HARMONIZATION ---
            # Now it will only apply UNKNOWN" to empy valid rows
            .with_columns([
                pl.when(
                    pl.col(col).is_null() | (pl.col(col).str.strip_chars() == "")
            )
            .then(pl.lit("UNKNOWN"))
            .otherwise(pl.col(col).str.strip_chars())
            .alias(col)
            for col in columns
        ])
    
        # --- PASO 5: DEDUPLICATION ---
        .unique(maintain_order=True)
    )

    # 3. Show trust results
    return cleaned_df