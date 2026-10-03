import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns


class DashboardRenderer:
  """Responsible solely for collecting aggregated metrics from LazyFrames

  and rendering the 2x2 executive operational dashboard.
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

    # 2. Set up visualization theme and figure canvas
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(18, 14))
    fig.suptitle(
        "Smart Grid Operational Pillars: Impact & Insights Dashboard",
        fontsize=20,
        fontweight="bold",
    )

    # Panel 1: Pillar 1 - Generation Capacity Mix
    sns.barplot(
        data=df_cap_agg.to_pandas(),
        x="asset_type",
        y="total_capacity",
        hue="unit",
        ax=axes[0, 0],
        palette="deep",
    )
    axes[0, 0].set_title(
        "Pillar 1: Capacity Mix (Normalized by Physical Unit)",
        fontsize=14,
        fontweight="bold",
    )
    axes[0, 0].tick_params(axis="x", rotation=30)

    # Panel 2: Pillar 2 - Grid Topology & Substation Density
    df_grid_melted = df_grid_agg.to_pandas().melt(
        id_vars=["provider_region", "substation_type"],
        value_vars=["avg_power_transformers", "avg_distribution_networks"],
        var_name="Asset_Metric",
        value_name="Average_Count",
    )
    sns.barplot(
        data=df_grid_melted,
        x="provider_region",
        y="Average_Count",
        hue="substation_type",
        ax=axes[0, 1],
        palette="muted",
    )
    axes[0, 1].set_title(
        "Pillar 2: Regional Substation Asset Density Bottlenecks",
        fontsize=14,
        fontweight="bold",
    )

    # Panel 3: Pillar 3 - AMI Multi-Tenancy & Third-Party Dependencies
    sns.barplot(
        data=df_meters_agg.to_pandas(),
        x="grid_provider_name",
        y="total_meters",
        hue="metering_provider_name",
        ax=axes[1, 0],
        palette="Set2",
    )
    axes[1, 0].set_title(
        "Pillar 3: AMI Multi-Tenancy & Third-Party Dependencies",
        fontsize=14,
        fontweight="bold",
    )
    axes[1, 0].tick_params(axis="x", rotation=25)

    # Panel 4: Pillar 4 - Meter Deployment Waves & Protocols
    sns.lineplot(
        data=df_lifecycle.to_pandas(),
        x="install_year",
        y="total_installed",
        hue="comm_protocol",
        marker="o",
        linewidth=2.5,
        ax=axes[1, 1],
    )
    axes[1, 1].set_title(
        "Pillar 4: Meter Cohort Deployment Waves & Protocol Evolution",
        fontsize=14,
        fontweight="bold",
    )
    axes[1, 1].tick_params(axis="x", rotation=45)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()