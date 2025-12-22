"""
ECharts Configuration Generator

Generates ECharts-compatible chart configurations from data analysis.
Replaces the Plotly-based visualization_generator for the interactive dashboard.
"""

from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
from enum import Enum


class ChartType(str, Enum):
    """Supported chart types"""
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    SCATTER = "scatter"
    AREA = "area"
    TREEMAP = "treemap"
    SUNBURST = "sunburst"
    HEATMAP = "heatmap"
    GAUGE = "gauge"


def generate_echarts_config(
    data: List[Dict],
    chart_type: ChartType,
    x_field: str,
    y_field: str,
    title: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Generate ECharts configuration from data.

    Args:
        data: List of data records
        chart_type: Type of chart to generate
        x_field: Field name for x-axis/categories
        y_field: Field name for y-axis/values
        title: Optional chart title
        **kwargs: Additional chart-specific options

    Returns:
        ECharts option configuration
    """

    generators = {
        ChartType.BAR: _generate_bar_config,
        ChartType.LINE: _generate_line_config,
        ChartType.PIE: _generate_pie_config,
        ChartType.SCATTER: _generate_scatter_config,
        ChartType.AREA: _generate_area_config,
        ChartType.TREEMAP: _generate_treemap_config,
        ChartType.HEATMAP: _generate_heatmap_config,
        ChartType.GAUGE: _generate_gauge_config,
    }

    generator = generators.get(chart_type, _generate_bar_config)
    return generator(data, x_field, y_field, title, **kwargs)


def _generate_bar_config(
    data: List[Dict],
    x_field: str,
    y_field: str,
    title: Optional[str] = None,
    horizontal: bool = False,
    stacked: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """Generate bar chart configuration"""

    categories = [str(d.get(x_field, '')) for d in data]
    values = [d.get(y_field, 0) for d in data]

    config = {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "shadow"}
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "top": "15%" if title else "8%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category" if not horizontal else "value",
            "data": categories if not horizontal else None
        },
        "yAxis": {
            "type": "value" if not horizontal else "category",
            "data": categories if horizontal else None
        },
        "series": [{
            "type": "bar",
            "data": values,
            "emphasis": {"focus": "series"},
            "itemStyle": {"borderRadius": [4, 4, 0, 0] if not horizontal else [0, 4, 4, 0]},
            "animationDelay": lambda idx: idx * 30
        }],
        "animationEasing": "elasticOut"
    }

    # Remove None values
    return {k: v for k, v in config.items() if v is not None}


def _generate_line_config(
    data: List[Dict],
    x_field: str,
    y_field: str,
    title: Optional[str] = None,
    smooth: bool = True,
    area: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """Generate line chart configuration"""

    categories = [str(d.get(x_field, '')) for d in data]
    values = [d.get(y_field, 0) for d in data]

    series_config = {
        "type": "line",
        "data": values,
        "smooth": smooth,
        "emphasis": {"focus": "series"},
        "symbol": "circle",
        "symbolSize": 6
    }

    if area:
        series_config["areaStyle"] = {"opacity": 0.3}

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {"trigger": "axis"},
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "top": "15%" if title else "8%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": categories,
            "boundaryGap": False
        },
        "yAxis": {"type": "value"},
        "series": [series_config]
    }


def _generate_pie_config(
    data: List[Dict],
    x_field: str,
    y_field: str,
    title: Optional[str] = None,
    donut: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """Generate pie chart configuration"""

    pie_data = [
        {"name": str(d.get(x_field, '')), "value": d.get(y_field, 0)}
        for d in data
    ]

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {
            "trigger": "item",
            "formatter": "{b}: {c} ({d}%)"
        },
        "legend": {
            "orient": "horizontal",
            "bottom": "5%"
        },
        "series": [{
            "type": "pie",
            "radius": ["40%", "70%"] if donut else "70%",
            "center": ["50%", "45%"],
            "data": pie_data,
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowOffsetX": 0,
                    "shadowColor": "rgba(0, 0, 0, 0.5)"
                }
            },
            "label": {
                "show": True,
                "formatter": "{b}: {d}%"
            },
            "animationType": "scale",
            "animationEasing": "elasticOut"
        }]
    }


def _generate_scatter_config(
    data: List[Dict],
    x_field: str,
    y_field: str,
    title: Optional[str] = None,
    size_field: Optional[str] = None,
    color_field: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """Generate scatter chart configuration"""

    scatter_data = [
        [d.get(x_field, 0), d.get(y_field, 0)]
        for d in data
    ]

    series_config = {
        "type": "scatter",
        "data": scatter_data,
        "symbolSize": 12,
        "emphasis": {
            "focus": "series",
            "itemStyle": {
                "shadowBlur": 10,
                "shadowColor": "rgba(0, 0, 0, 0.5)"
            }
        }
    }

    if size_field:
        sizes = [d.get(size_field, 10) for d in data]
        max_size = max(sizes) if sizes else 1
        series_config["symbolSize"] = lambda idx: (sizes[idx] / max_size) * 40 + 5

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {
            "trigger": "item",
            "formatter": lambda params: f"{x_field}: {params['value'][0]}<br/>{y_field}: {params['value'][1]}"
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "top": "15%" if title else "8%",
            "containLabel": True
        },
        "xAxis": {
            "type": "value",
            "name": x_field
        },
        "yAxis": {
            "type": "value",
            "name": y_field
        },
        "series": [series_config]
    }


def _generate_area_config(
    data: List[Dict],
    x_field: str,
    y_field: str,
    title: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """Generate area chart configuration"""
    return _generate_line_config(data, x_field, y_field, title, smooth=True, area=True, **kwargs)


def _generate_treemap_config(
    data: List[Dict],
    name_field: str,
    value_field: str,
    title: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """Generate treemap configuration for hierarchical data"""

    treemap_data = [
        {"name": str(d.get(name_field, '')), "value": d.get(value_field, 0)}
        for d in data
    ]

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {
            "trigger": "item",
            "formatter": "{b}: {c}"
        },
        "series": [{
            "type": "treemap",
            "data": treemap_data,
            "leafDepth": 1,
            "roam": False,
            "label": {
                "show": True,
                "formatter": "{b}"
            },
            "upperLabel": {
                "show": True,
                "height": 30
            },
            "itemStyle": {
                "borderColor": "#0f1428",
                "borderWidth": 2,
                "gapWidth": 2
            },
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 20,
                    "shadowColor": "rgba(0, 0, 0, 0.4)"
                }
            }
        }]
    }


def _generate_heatmap_config(
    data: List[Dict],
    x_field: str,
    y_field: str,
    value_field: str = "value",
    title: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """Generate heatmap configuration"""

    x_categories = sorted(set(str(d.get(x_field, '')) for d in data))
    y_categories = sorted(set(str(d.get(y_field, '')) for d in data))

    heatmap_data = []
    values = []
    for d in data:
        x_idx = x_categories.index(str(d.get(x_field, '')))
        y_idx = y_categories.index(str(d.get(y_field, '')))
        value = d.get(value_field, 0)
        heatmap_data.append([x_idx, y_idx, value])
        values.append(value)

    min_val = min(values) if values else 0
    max_val = max(values) if values else 100

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {
            "position": "top"
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "15%",
            "top": "15%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": x_categories,
            "splitArea": {"show": True}
        },
        "yAxis": {
            "type": "category",
            "data": y_categories,
            "splitArea": {"show": True}
        },
        "visualMap": {
            "min": min_val,
            "max": max_val,
            "calculable": True,
            "orient": "horizontal",
            "left": "center",
            "bottom": "0%",
            "inRange": {
                "color": ["#1a1f35", "#7dd3fc"]
            }
        },
        "series": [{
            "type": "heatmap",
            "data": heatmap_data,
            "label": {"show": True},
            "emphasis": {
                "itemStyle": {
                    "shadowBlur": 10,
                    "shadowColor": "rgba(0, 0, 0, 0.5)"
                }
            }
        }]
    }


def _generate_gauge_config(
    data: List[Dict],
    value_field: str,
    max_value: float = 100,
    title: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """Generate gauge chart configuration"""

    value = data[0].get(value_field, 0) if data else 0

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {"formatter": "{b}: {c}"},
        "series": [{
            "type": "gauge",
            "progress": {"show": True, "width": 18},
            "axisLine": {
                "lineStyle": {"width": 18}
            },
            "axisTick": {"show": False},
            "splitLine": {
                "length": 15,
                "lineStyle": {"width": 2, "color": "#9ca3af"}
            },
            "axisLabel": {
                "distance": 25,
                "color": "#9ca3af",
                "fontSize": 12
            },
            "anchor": {
                "show": True,
                "showAbove": True,
                "size": 25,
                "itemStyle": {"borderWidth": 10}
            },
            "title": {"show": False},
            "detail": {
                "valueAnimation": True,
                "fontSize": 32,
                "offsetCenter": [0, "70%"],
                "color": "#ffffff"
            },
            "data": [{"value": value, "name": title or "Value"}],
            "max": max_value
        }]
    }


def suggest_chart_type(df: pd.DataFrame, x_col: str, y_col: str) -> ChartType:
    """
    Suggest the best chart type based on data characteristics.

    Args:
        df: DataFrame with data
        x_col: X-axis column name
        y_col: Y-axis column name

    Returns:
        Recommended ChartType
    """

    if x_col not in df.columns or y_col not in df.columns:
        return ChartType.BAR

    x_dtype = df[x_col].dtype
    y_dtype = df[y_col].dtype

    # Check if x is temporal
    if pd.api.types.is_datetime64_any_dtype(x_dtype):
        return ChartType.LINE

    # Check cardinality
    x_cardinality = df[x_col].nunique()

    # Low cardinality categorical -> pie chart
    if x_cardinality <= 6 and x_cardinality >= 2:
        return ChartType.PIE

    # Moderate cardinality -> bar chart
    if x_cardinality <= 20:
        return ChartType.BAR

    # High cardinality numeric data -> scatter
    if pd.api.types.is_numeric_dtype(x_dtype) and pd.api.types.is_numeric_dtype(y_dtype):
        return ChartType.SCATTER

    # Default to bar
    return ChartType.BAR


def generate_multi_series_config(
    data: List[Dict],
    x_field: str,
    y_fields: List[str],
    chart_type: ChartType = ChartType.LINE,
    title: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Generate multi-series chart configuration.

    Args:
        data: List of data records
        x_field: Field for x-axis
        y_fields: List of fields for series
        chart_type: Type of chart (line or bar)
        title: Chart title

    Returns:
        ECharts configuration
    """

    categories = [str(d.get(x_field, '')) for d in data]

    series = []
    for y_field in y_fields:
        values = [d.get(y_field, 0) for d in data]
        series_config = {
            "name": y_field,
            "type": chart_type.value,
            "data": values,
            "emphasis": {"focus": "series"}
        }

        if chart_type == ChartType.LINE:
            series_config["smooth"] = True
            series_config["symbol"] = "circle"
            series_config["symbolSize"] = 6
        elif chart_type == ChartType.BAR:
            series_config["itemStyle"] = {"borderRadius": [4, 4, 0, 0]}

        series.append(series_config)

    return {
        "title": {"text": title, "left": "center"} if title else None,
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "shadow" if chart_type == ChartType.BAR else "line"}
        },
        "legend": {
            "data": y_fields,
            "bottom": "0%"
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "12%",
            "top": "15%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": categories,
            "boundaryGap": chart_type == ChartType.BAR
        },
        "yAxis": {"type": "value"},
        "series": series
    }


def add_interactive_features(
    config: Dict[str, Any],
    enable_zoom: bool = False,
    enable_brush: bool = False,
    enable_toolbox: bool = True
) -> Dict[str, Any]:
    """
    Add interactive features to an existing chart configuration.

    Args:
        config: Existing ECharts configuration
        enable_zoom: Enable data zoom slider
        enable_brush: Enable brush selection
        enable_toolbox: Enable toolbox features

    Returns:
        Enhanced configuration
    """

    if enable_toolbox:
        config["toolbox"] = {
            "feature": {
                "saveAsImage": {"title": "Save"},
                "dataView": {"title": "Data", "readOnly": True},
                "restore": {"title": "Reset"}
            }
        }

    if enable_zoom:
        config["dataZoom"] = [
            {
                "type": "inside",
                "start": 0,
                "end": 100
            },
            {
                "type": "slider",
                "start": 0,
                "end": 100,
                "bottom": "0%"
            }
        ]
        # Adjust grid for zoom slider
        if "grid" in config:
            config["grid"]["bottom"] = "15%"

    if enable_brush:
        config["brush"] = {
            "toolbox": ["rect", "polygon", "keep", "clear"],
            "brushLink": "all",
            "throttleType": "debounce",
            "throttleDelay": 300
        }
        if "toolbox" in config and "feature" in config["toolbox"]:
            config["toolbox"]["feature"]["brush"] = {
                "type": ["rect", "polygon", "clear"]
            }

    return config
