import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns


class DashboardRenderer:
  """Responsible for collecting aggregated metrics from LazyFrames

  and rendering a clean, minimalist, insight-driven executive dashboard.
  """

  def __init__(
      self,
      lf_capacity: pl.LazyFrame,
      lf_grid: pl.LazyFrame,
      lf_meters: pl.LazyFrame,
  ):
    self.lf_capacity = lf_capacity
    self.lf_grid = lf_grid
    self.lf_meters = lf_meters

  def render_executive_dashboard(self):
    # 1. Collect lightweight aggregated summaries lazily
    df_cap_agg = (
        self.lf_capacity.group_by(["domain", "asset_type", "unit"])
        .agg(pl.col("value").sum().alias("total_capacity"))
        .sort("total_capacity", descending=True)
        .collect()
    )

    df_grid_agg = (
        self.lf_grid.group_by(["provider_region", "substation_type"])
        .agg([
            pl.col("power_transformer_count")
            .mean()
            .alias("avg_power_transformers"),
            pl.col("distribution_network_count")
            .mean()
            .alias("avg_distribution_networks"),
        ])
        .collect()
    )

    df_meters_agg = (
        self.lf_meters.group_by(["grid_provider_name", "metering_provider_name"])
        .agg(pl.count("meter_id").alias("total_meters"))
        .sort("total_meters", descending=True)
        .collect()
    )

    df_lifecycle = (
        self.lf_meters.with_columns(
            pl.col("install_date").str.slice(0, 4).alias("install_year")
        )
        .group_by(["install_year", "comm_protocol"])
        .agg(pl.count("meter_id").alias("total_installed"))
        .sort("install_year")
        .collect()
    )

    # Convert to pandas and apply business-friendly formatting
    pdf_cap = df_cap_agg.to_pandas()
    pdf_cap["asset_type"] = (
        pdf_cap["asset_type"].str.replace("_", " ").str.title()
    )

    pdf_grid = df_grid_agg.to_pandas()
    pdf_grid_melted = pdf_grid.melt(
        id_vars=["provider_region", "substation_type"],
        value_vars=["avg_power_transformers", "avg_distribution_networks"],
        var_name="Asset_Metric",
        value_name="Average_Count",
    )
    pdf_grid_melted["Asset_Metric"] = pdf_grid_melted["Asset_Metric"].replace({
        "avg_power_transformers": "Avg Power Transformers",
        "avg_distribution_networks": "Avg Distribution Networks",
    })

    pdf_meters = df_meters_agg.to_pandas()

    pdf_lifecycle = df_lifecycle.to_pandas()
    pdf_lifecycle["comm_protocol"] = pdf_lifecycle["comm_protocol"].str.upper()

    # 2. Set up clean minimal visualization theme
    sns.set_theme(style="ticks")
    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle(
        "Smart Grid Operational Pillars: Executive Impact Dashboard",
        fontsize=20,
        fontweight="bold",
        y=0.96,
    )

    # --- Panel 1: Pillar 1 - Generation & Asset Capacity Mix (Using Qualitative Palette) ---
    sns.barplot(
        data=pdf_cap,
        y="asset_type",
        x="total_capacity",
        hue="unit",
        orient="h",
        ax=axes[0, 0],
        palette="Set2",  # Qualitative palette avoids false magnitude implications
    )
    axes[0, 0].set_title(
        "Pillar 1: Generation & Asset Capacity Mix (Log Scale)",
        fontsize=13,
        fontweight="bold",
    )
    axes[0, 0].set_xlabel("Total Capacity (Log Scale)", fontsize=11)
    axes[0, 0].set_ylabel("Asset Type", fontsize=11)
    axes[0, 0].set_xscale("log")
    axes[0, 0].legend(title="Unit", loc="lower right", frameon=False)
    sns.despine(ax=axes[0, 0], top=True, right=True)

    # --- Panel 2: Pillar 2 - Grid Topology & Substation Density ---
    sns.barplot(
        data=pdf_grid_melted,
        x="provider_region",
        y="Average_Count",
        hue="substation_type",
        ax=axes[0, 1],
        palette="Blues_d",
    )
    axes[0, 1].set_title(
        "Pillar 2: Regional Substation Density Bottlenecks",
        fontsize=13,
        fontweight="bold",
    )
    axes[0, 1].set_xlabel("Provider Region", fontsize=11)
    axes[0, 1].set_ylabel("Average Asset Count", fontsize=11)
    axes[0, 1].legend(title="Substation Type", frameon=False)
    sns.despine(ax=axes[0, 1], top=True, right=True)

    # --- Panel 3: Pillar 3 - AMI Multi-Tenancy & Third-Party Vendors ---
    sns.barplot(
        data=pdf_meters,
        y="grid_provider_name",
        x="total_meters",
        hue="metering_provider_name",
        orient="h",
        ax=axes[1, 0],
        palette="Blues_r",
    )
    axes[1, 0].set_title(
        "Pillar 3: AMI Multi-Tenancy Vendor Ecosystem",
        fontsize=13,
        fontweight="bold",
    )
    axes[1, 0].set_xlabel("Total Connected Meters", fontsize=11)
    axes[1, 0].set_ylabel("Grid Provider", fontsize=11)
    axes[1, 0].legend(title="Metering Provider", loc="lower right", frameon=False)
    sns.despine(ax=axes[1, 0], top=True, right=True)

    # --- Panel 4: Pillar 4 - Clean Focal Trend Analysis ---
    sns.lineplot(
        data=pdf_lifecycle,
        x="install_year",
        y="total_installed",
        hue="comm_protocol",
        style="comm_protocol",
        markers=True,
        linewidth=2.2,
        ax=axes[1, 1],
        palette="tab10",
    )
    axes[1, 1].set_title(
        "Pillar 4: Protocol Cohort Evolution & Deployment Waves",
        fontsize=13,
        fontweight="bold",
    )
    axes[1, 1].set_xlabel("Installation Year", fontsize=11)
    axes[1, 1].set_ylabel("Total Meters Installed", fontsize=11)
    axes[1, 1].tick_params(axis="x", rotation=30)
    axes[1, 1].legend(
        title="Protocol", frameon=False, bbox_to_anchor=(1.02, 1), loc="upper left"
    )
    sns.despine(ax=axes[1, 1], top=True, right=True)

    plt.tight_layout(rect=[0, 0.03, 0.92, 0.94])
    plt.show()