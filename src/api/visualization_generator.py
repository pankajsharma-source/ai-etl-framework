"""
Visualization Generator Service

Auto-generates appropriate Plotly charts from Parquet/CSV data files.
Analyzes column types and relationships to select the best chart types.
"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Any, Optional, Tuple
import json
import base64
import logging

logger = logging.getLogger(__name__)


def decode_plotly_binary(obj):
    """
    Recursively decode binary-encoded arrays in Plotly JSON.
    Plotly 6.x encodes numpy arrays as {'dtype': 'xx', 'bdata': 'base64...'}.
    This function converts them back to regular lists.
    """
    if isinstance(obj, dict):
        if 'bdata' in obj and 'dtype' in obj:
            # This is a binary-encoded array
            try:
                dtype_map = {
                    'i1': np.int8, 'i2': np.int16, 'i4': np.int32, 'i8': np.int64,
                    'u1': np.uint8, 'u2': np.uint16, 'u4': np.uint32, 'u8': np.uint64,
                    'f4': np.float32, 'f8': np.float64
                }
                dtype = dtype_map.get(obj['dtype'], np.float64)
                data = base64.b64decode(obj['bdata'])
                arr = np.frombuffer(data, dtype=dtype)
                return arr.tolist()
            except Exception as e:
                logger.warning(f"Failed to decode binary array: {e}")
                return obj
        else:
            return {k: decode_plotly_binary(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_plotly_binary(item) for item in obj]
    else:
        return obj

# Dark theme template matching AI Studio UI
DARK_THEME = {
    'layout': {
        'paper_bgcolor': 'rgba(15, 20, 40, 0.95)',
        'plot_bgcolor': 'rgba(15, 20, 40, 0.95)',
        'font': {'color': '#ffffff', 'family': 'Inter, system-ui, sans-serif'},
        'title': {'font': {'size': 16, 'color': '#ffffff'}},
        'xaxis': {
            'gridcolor': 'rgba(45, 55, 72, 0.5)',
            'linecolor': 'rgba(45, 55, 72, 0.8)',
            'tickcolor': '#9ca3af',
            'tickfont': {'color': '#9ca3af'}
        },
        'yaxis': {
            'gridcolor': 'rgba(45, 55, 72, 0.5)',
            'linecolor': 'rgba(45, 55, 72, 0.8)',
            'tickcolor': '#9ca3af',
            'tickfont': {'color': '#9ca3af'}
        },
        'legend': {'font': {'color': '#9ca3af'}},
        # Soft, muted color palette - easier on the eyes
        'colorway': ['#7dd3fc', '#86efac', '#fcd34d', '#fca5a5', '#c4b5fd', '#f9a8d4', '#a5f3fc', '#bef264'],
        'margin': {'l': 60, 'r': 30, 't': 50, 'b': 60}
    }
}


def analyze_dataframe(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Analyze DataFrame columns and return metadata for chart selection.

    Returns:
        Dict mapping column names to their analysis:
        {
            'column_name': {
                'dtype': str,           # 'numeric', 'categorical', 'datetime', 'boolean'
                'unique_count': int,
                'null_count': int,
                'sample_values': list,
                'min': Any,
                'max': Any,
                'mean': float (for numeric),
                'is_id': bool,          # Likely an ID column
                'is_geographic': bool   # Contains state/region data
            }
        }
    """
    analysis = {}

    for col in df.columns:
        col_data = df[col]
        info = {
            'dtype': 'unknown',
            'unique_count': col_data.nunique(),
            'null_count': col_data.isnull().sum(),
            'sample_values': col_data.dropna().head(5).tolist(),
            'total_count': len(col_data)
        }

        # Determine column type
        if pd.api.types.is_numeric_dtype(col_data):
            info['dtype'] = 'numeric'
            info['min'] = float(col_data.min()) if not pd.isna(col_data.min()) else None
            info['max'] = float(col_data.max()) if not pd.isna(col_data.max()) else None
            info['mean'] = float(col_data.mean()) if not pd.isna(col_data.mean()) else None
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            info['dtype'] = 'datetime'
            info['min'] = str(col_data.min())
            info['max'] = str(col_data.max())
        elif pd.api.types.is_bool_dtype(col_data):
            info['dtype'] = 'boolean'
        else:
            # Check if it's a date string
            try:
                pd.to_datetime(col_data.dropna().head(100))
                info['dtype'] = 'datetime'
            except:
                info['dtype'] = 'categorical'

        # Check if likely an ID column
        # Only use uniqueness as ID signal for larger datasets (>20 rows)
        # to avoid flagging all columns as IDs in small master data tables
        # But don't flag monetary/value columns as IDs even if unique
        monetary_terms = ['spend', 'amount', 'total', 'sum', 'revenue', 'sales', 'cost', 'price', 'payment', 'balance', 'income', 'profit', 'loss', 'fee', 'charge', 'value', 'worth']
        is_monetary = any(term in col.lower() for term in monetary_terms)

        info['is_id'] = (
            ('id' in col.lower() or '_id' in col.lower() or col.lower().endswith('_id')) and not is_monetary
        ) or (
            # Uniqueness-based ID detection - only for non-monetary columns
            len(df) > 20 and
            info['unique_count'] == len(df) and
            info['dtype'] == 'categorical' and  # Only categorical, not numeric (numeric unique values are often valid data)
            not is_monetary
        )

        # Check if boolean-like column (should not be aggregated as numeric)
        # Detect by name pattern or by having only 0/1 or True/False values
        boolean_name_patterns = ['is_', 'has_', 'can_', 'should_', 'was_', 'will_', 'did_', 'does_', 'active', 'enabled', 'disabled', 'valid', 'verified']
        is_boolean_name = any(col.lower().startswith(p) or col.lower().endswith(p.rstrip('_')) for p in boolean_name_patterns)

        # Check if values are binary (only 0/1 or True/False)
        is_binary_values = False
        if info['dtype'] == 'numeric' and info['unique_count'] <= 2:
            unique_vals = set(col_data.dropna().unique())
            is_binary_values = unique_vals.issubset({0, 1, 0.0, 1.0, True, False})

        info['is_boolean_like'] = is_boolean_name or is_binary_values or info['dtype'] == 'boolean'

        # Check if percentage/rate column (summing percentages is meaningless)
        # Be careful: "hourly_rate", "pay_rate" are monetary values, NOT percentages
        # Only flag as rate if it's clearly a percentage/ratio type
        rate_patterns = ['percent', 'pct', 'ratio', 'score', 'rating', 'rank', 'conversion_rate', 'interest_rate', 'tax_rate', 'discount_rate']
        # Exclude monetary rates like hourly_rate, pay_rate, billing_rate
        monetary_rate_patterns = ['hourly_rate', 'pay_rate', 'billing_rate', 'wage_rate', 'salary_rate']
        is_monetary_rate = any(p in col.lower() for p in monetary_rate_patterns)
        info['is_rate'] = any(p in col.lower() for p in rate_patterns) and not is_monetary_rate

        # Check if unit price column (should average, not sum)
        unit_price_patterns = ['unit_price', 'price_per', 'cost_per', 'rate_per', 'per_unit', 'each']
        info['is_unit_price'] = any(p in col.lower() for p in unit_price_patterns)

        # Check if code column (looks numeric but is categorical)
        code_patterns = ['zip', 'postal', 'phone', 'sku', 'upc', 'barcode', 'code', 'pin']
        is_code_by_name = any(p in col.lower() for p in code_patterns)
        # Exclude measurement columns that might look like codes
        measurement_patterns = ['square', 'footage', 'area', 'size', 'length', 'width', 'height', 'weight', 'volume', 'capacity', 'distance']
        is_measurement = any(p in col.lower() for p in measurement_patterns)
        info['is_code'] = is_code_by_name and not is_measurement

        # Check if year/month column (should not be summed)
        year_month_patterns = ['year', 'month', 'yr', 'mo', '_yy', '_mm']
        is_year_month_by_name = any(p in col.lower() for p in year_month_patterns)
        # Detect year values (1900-2100 range, few unique values)
        is_year_by_value = False
        if info['dtype'] == 'numeric' and info['unique_count'] <= 50:
            if info.get('min') and info.get('max'):
                if 1900 <= info['min'] <= 2100 and 1900 <= info['max'] <= 2100:
                    is_year_by_value = True
        info['is_year_month'] = is_year_month_by_name or is_year_by_value

        # Check if sequential/index column
        index_patterns = ['index', 'idx', 'row_num', 'row_number', 'sequence', 'seq', 'line_num']
        info['is_index'] = any(p in col.lower() for p in index_patterns)

        # Mark as non-aggregatable if any of the above
        info['is_non_aggregatable'] = (
            info['is_boolean_like'] or
            info['is_rate'] or
            info['is_unit_price'] or
            info['is_code'] or
            info['is_year_month'] or
            info['is_index']
        )

        # Check if geographic
        geographic_terms = ['state', 'region', 'country', 'city', 'location', 'zip', 'postal']
        info['is_geographic'] = any(term in col.lower() for term in geographic_terms)

        # US state detection
        us_states = {'CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI',
                     'AZ', 'WA', 'CO', 'NV', 'OR', 'MA', 'VA', 'NJ', 'MD', 'TN'}
        if info['dtype'] == 'categorical' and info['unique_count'] <= 52:
            sample_set = set(str(v).upper() for v in info['sample_values'])
            if sample_set & us_states:
                info['is_geographic'] = True
                info['geographic_type'] = 'us_state'

        analysis[col] = info

    return analysis


def select_chart_types(analysis: Dict[str, Dict], df: pd.DataFrame, max_charts: int = 10) -> List[Dict[str, Any]]:
    """
    Select appropriate chart types based on column analysis.

    Returns list of chart configurations:
    [
        {
            'chart_type': str,
            'title': str,
            'x_column': str,
            'y_column': str (optional),
            'color_column': str (optional),
            'priority': int (1-10, higher = more important)
        }
    ]
    """
    charts = []

    # Separate columns by type
    # Exclude non-aggregatable columns from numeric - they shouldn't be summed
    # (booleans, percentages, rates, codes, years, etc.)
    numeric_cols = [c for c, info in analysis.items()
                    if info['dtype'] == 'numeric' and not info['is_id'] and not info.get('is_non_aggregatable')]
    categorical_cols = [c for c, info in analysis.items()
                        if info['dtype'] == 'categorical' and not info['is_id']
                        and info['unique_count'] <= 20]
    datetime_cols = [c for c, info in analysis.items()
                     if info['dtype'] == 'datetime']
    geographic_cols = [c for c, info in analysis.items()
                       if info.get('is_geographic') and info['dtype'] == 'categorical']
    # Boolean columns can be used for pie/distribution charts (count-based)
    boolean_cols = [c for c, info in analysis.items()
                    if info.get('is_boolean_like') and not info['is_id']]
    # Code columns should be treated as categorical for charts
    code_cols = [c for c, info in analysis.items()
                 if info.get('is_code') and not info['is_id']]

    # 1. Bar charts: Categorical + Numeric (highest priority for business data)
    for cat_col in categorical_cols[:3]:
        for num_col in numeric_cols[:3]:
            if analysis[cat_col]['unique_count'] <= 15:
                charts.append({
                    'chart_type': 'bar',
                    'title': f'{num_col} by {cat_col}',
                    'x_column': cat_col,
                    'y_column': num_col,
                    'priority': 9
                })

    # 2. Line charts: DateTime + Numeric (great for trends)
    for dt_col in datetime_cols[:2]:
        for num_col in numeric_cols[:3]:
            charts.append({
                'chart_type': 'line',
                'title': f'{num_col} over Time',
                'x_column': dt_col,
                'y_column': num_col,
                'priority': 8
            })

    # 3. Pie charts: Single categorical with few values
    for cat_col in categorical_cols:
        if 3 <= analysis[cat_col]['unique_count'] <= 8:
            charts.append({
                'chart_type': 'pie',
                'title': f'Distribution by {cat_col}',
                'x_column': cat_col,
                'priority': 6
            })
            break  # Only one pie chart

    # 3b. Pie charts for boolean columns (True/False distribution is meaningful)
    for bool_col in boolean_cols[:1]:  # Only one boolean pie chart
        charts.append({
            'chart_type': 'pie',
            'title': f'{bool_col} Distribution',
            'x_column': bool_col,
            'priority': 5
        })

    # 4. Histograms: Numeric distributions
    for num_col in numeric_cols[:2]:
        if analysis[num_col]['unique_count'] > 10:
            charts.append({
                'chart_type': 'histogram',
                'title': f'{num_col} Distribution',
                'x_column': num_col,
                'priority': 5
            })

    # 5. Scatter plots: Two numeric columns (correlations)
    if len(numeric_cols) >= 2:
        charts.append({
            'chart_type': 'scatter',
            'title': f'{numeric_cols[0]} vs {numeric_cols[1]}',
            'x_column': numeric_cols[0],
            'y_column': numeric_cols[1],
            'color_column': categorical_cols[0] if categorical_cols else None,
            'priority': 7
        })

    # 6. Box plots: Numeric by category (show distributions)
    for cat_col in categorical_cols[:1]:
        for num_col in numeric_cols[:2]:
            if analysis[cat_col]['unique_count'] <= 10:
                charts.append({
                    'chart_type': 'box',
                    'title': f'{num_col} by {cat_col}',
                    'x_column': cat_col,
                    'y_column': num_col,
                    'priority': 4
                })

    # 7. Heatmap: Two categorical + numeric
    # Only create heatmap if there's actual cross-tabulation (not 1-to-1 relationship)
    if len(categorical_cols) >= 2 and len(numeric_cols) >= 1:
        cat1, cat2 = categorical_cols[0], categorical_cols[1]
        if analysis[cat1]['unique_count'] <= 10 and analysis[cat2]['unique_count'] <= 10:
            # Check if columns have a 1-to-1 relationship (useless for heatmap)
            # If unique combinations equals both unique counts, it's likely 1-to-1
            unique_combinations = df[[cat1, cat2]].drop_duplicates().shape[0]
            min_unique = min(analysis[cat1]['unique_count'], analysis[cat2]['unique_count'])
            # Only create heatmap if there's actual cross-tabulation
            # (multiple values per cell, not just a diagonal)
            if unique_combinations < min_unique or len(df) > unique_combinations * 1.5:
                charts.append({
                    'chart_type': 'heatmap',
                    'title': f'{numeric_cols[0]} by {cat1} and {cat2}',
                    'x_column': cat1,
                    'y_column': cat2,
                    'z_column': numeric_cols[0],
                    'priority': 5
                })

    # Sort by priority and limit
    charts.sort(key=lambda x: x['priority'], reverse=True)
    return charts[:max_charts]


def generate_plotly_chart(df: pd.DataFrame, chart_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a Plotly chart based on configuration.

    Returns Plotly figure as JSON dict.
    """
    chart_type = chart_config['chart_type']
    title = chart_config['title']
    x_col = chart_config.get('x_column')
    y_col = chart_config.get('y_column')
    color_col = chart_config.get('color_column')
    z_col = chart_config.get('z_column')

    # Soft color palette for charts
    soft_colors = ['#7dd3fc', '#86efac', '#fcd34d', '#fca5a5', '#c4b5fd', '#f9a8d4', '#a5f3fc', '#bef264']

    try:
        if chart_type == 'bar':
            # Aggregate data for bar chart
            agg_df = df.groupby(x_col)[y_col].sum().reset_index()
            agg_df = agg_df.sort_values(y_col, ascending=False).head(15)
            fig = px.bar(agg_df, x=x_col, y=y_col, title=title, color_discrete_sequence=soft_colors)

        elif chart_type == 'line':
            # Sort by date for line chart
            df_sorted = df.copy()
            df_sorted[x_col] = pd.to_datetime(df_sorted[x_col])
            agg_df = df_sorted.groupby(x_col)[y_col].sum().reset_index()
            agg_df = agg_df.sort_values(x_col)
            fig = px.line(agg_df, x=x_col, y=y_col, title=title, markers=True, color_discrete_sequence=soft_colors)

        elif chart_type == 'pie':
            value_counts = df[x_col].value_counts().head(10)
            values = value_counts.values.tolist()
            labels = value_counts.index.tolist()

            # Convert boolean labels to human-readable strings
            # Use column name to create meaningful labels (e.g., is_active -> Active/Inactive)
            if all(isinstance(l, (bool, np.bool_)) or l in [0, 1, True, False] for l in labels):
                # Extract the meaningful part of the column name
                col_name = x_col.lower().replace('is_', '').replace('has_', '').replace('_', ' ').title()
                label_map = {
                    True: col_name,
                    False: f'Not {col_name}',
                    1: col_name,
                    0: f'Not {col_name}'
                }
                labels = [label_map.get(l, str(l)) for l in labels]

            fig = px.pie(values=values, names=labels, title=title, color_discrete_sequence=soft_colors)

            # Improve label positioning to avoid overlap
            fig.update_traces(
                textposition='inside',
                textinfo='percent',  # Show only percentage inside
                insidetextorientation='horizontal',
            )
            # Hide legend since labels are shown, and adjust margins
            fig.update_layout(
                showlegend=True,
                legend=dict(
                    orientation='h',
                    yanchor='top',
                    y=-0.15,  # Position below the chart
                    xanchor='center',
                    x=0.5,
                    font=dict(size=10)
                ),
                margin=dict(b=80)  # Add bottom margin for legend
            )

        elif chart_type == 'histogram':
            fig = px.histogram(df, x=x_col, title=title, nbins=30, color_discrete_sequence=soft_colors)

        elif chart_type == 'scatter':
            sample_df = df.sample(min(1000, len(df)))  # Limit points for performance
            fig = px.scatter(sample_df, x=x_col, y=y_col, color=color_col,
                           title=title, opacity=0.7, color_discrete_sequence=soft_colors)

        elif chart_type == 'box':
            fig = px.box(df, x=x_col, y=y_col, title=title, color_discrete_sequence=soft_colors)

        elif chart_type == 'heatmap':
            pivot_df = df.pivot_table(values=z_col, index=y_col, columns=x_col, aggfunc='sum')
            # Use soft blue-purple gradient for heatmap
            fig = px.imshow(pivot_df, title=title, aspect='auto',
                          color_continuous_scale=['#1e1b4b', '#4338ca', '#7dd3fc'])
        else:
            logger.warning(f"Unknown chart type: {chart_type}")
            return None

        # Apply dark theme
        fig.update_layout(**DARK_THEME['layout'])

        # Convert to JSON and decode any binary-encoded arrays
        # (Plotly 6.x encodes numpy arrays as binary which frontends can't decode)
        chart_json = json.loads(fig.to_json())
        return decode_plotly_binary(chart_json)

    except Exception as e:
        logger.error(f"Error generating {chart_type} chart: {e}")
        return None


def generate_all_charts(file_path: str, max_charts: int = 10) -> List[Dict[str, Any]]:
    """
    Main entry point: Analyze a data file and generate all appropriate charts.

    Args:
        file_path: Path to Parquet or CSV file
        max_charts: Maximum number of charts to generate

    Returns:
        List of chart dictionaries with:
        {
            'chart_type': str,
            'title': str,
            'x_column': str,
            'y_column': str (optional),
            'chart_config': dict (Plotly JSON)
        }
    """
    logger.info(f"Generating visualizations for: {file_path}")

    # Load data
    try:
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            logger.error(f"Unsupported file format: {file_path}")
            return []
    except Exception as e:
        logger.error(f"Failed to load file {file_path}: {e}")
        return []

    if df.empty:
        logger.warning("DataFrame is empty, no charts to generate")
        return []

    # Analyze columns
    analysis = analyze_dataframe(df)
    logger.info(f"Analyzed {len(analysis)} columns")

    # Select chart types
    chart_configs = select_chart_types(analysis, df, max_charts)
    logger.info(f"Selected {len(chart_configs)} chart types")

    # Generate charts
    charts = []
    for config in chart_configs:
        chart_json = generate_plotly_chart(df, config)
        if chart_json:
            charts.append({
                'chart_type': config['chart_type'],
                'title': config['title'],
                'x_column': config.get('x_column'),
                'y_column': config.get('y_column'),
                'chart_config': chart_json
            })

    logger.info(f"Successfully generated {len(charts)} charts")
    return charts


def generate_custom_chart(file_path: str, prompt: str) -> Optional[Dict[str, Any]]:
    """
    Generate a custom chart based on user prompt.
    Uses simple keyword matching to interpret the request.

    Args:
        file_path: Path to data file
        prompt: User's natural language request (e.g., "bar chart of sales by region")

    Returns:
        Single chart dictionary or None
    """
    # Load data
    try:
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            return None
    except Exception as e:
        logger.error(f"Failed to load file: {e}")
        return None

    prompt_lower = prompt.lower()
    columns_lower = {col.lower(): col for col in df.columns}

    # Detect chart type from prompt
    chart_type = 'bar'  # default
    if any(word in prompt_lower for word in ['line', 'trend', 'over time', 'timeline']):
        chart_type = 'line'
    elif any(word in prompt_lower for word in ['pie', 'distribution', 'breakdown']):
        chart_type = 'pie'
    elif any(word in prompt_lower for word in ['scatter', 'correlation', 'vs', 'versus']):
        chart_type = 'scatter'
    elif any(word in prompt_lower for word in ['histogram', 'distribution']):
        chart_type = 'histogram'
    elif any(word in prompt_lower for word in ['box', 'boxplot']):
        chart_type = 'box'
    elif any(word in prompt_lower for word in ['heatmap', 'heat map']):
        chart_type = 'heatmap'

    # Find columns mentioned in prompt
    mentioned_cols = []
    for col_lower, col_original in columns_lower.items():
        # Check for exact match or partial match
        if col_lower in prompt_lower or col_lower.replace('_', ' ') in prompt_lower:
            mentioned_cols.append(col_original)

    # If no columns found, use analysis to pick best ones
    if not mentioned_cols:
        analysis = analyze_dataframe(df)
        numeric_cols = [c for c, info in analysis.items()
                        if info['dtype'] == 'numeric' and not info['is_id']]
        categorical_cols = [c for c, info in analysis.items()
                            if info['dtype'] == 'categorical' and not info['is_id']]

        if chart_type in ['bar', 'pie'] and categorical_cols:
            mentioned_cols = [categorical_cols[0]]
            if numeric_cols:
                mentioned_cols.append(numeric_cols[0])
        elif chart_type in ['line', 'histogram'] and numeric_cols:
            mentioned_cols = [numeric_cols[0]]

    # Build chart config
    config = {
        'chart_type': chart_type,
        'title': prompt[:50] + ('...' if len(prompt) > 50 else ''),
        'x_column': mentioned_cols[0] if mentioned_cols else df.columns[0],
        'y_column': mentioned_cols[1] if len(mentioned_cols) > 1 else None
    }

    # Generate the chart
    chart_json = generate_plotly_chart(df, config)
    if chart_json:
        return {
            'chart_type': config['chart_type'],
            'title': config['title'],
            'x_column': config['x_column'],
            'y_column': config.get('y_column'),
            'chart_config': chart_json
        }

    return None
