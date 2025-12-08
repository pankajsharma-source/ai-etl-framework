"""
AI-Powered Insight Generator for Data Sources

Generates data summaries and key insights using OpenAI API
after ETL or RAG pipeline processing completes.
"""

import os
import json
import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class InsightGenerator:
    """Generates AI-powered insights from processed data files."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the insight generator.

        Args:
            api_key: OpenAI API key. If not provided, reads from OPENAI_API_KEY env var.
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")

        # Import OpenAI here to avoid issues if not installed
        from openai import OpenAI
        self.client = OpenAI(api_key=self.api_key)

        # Default model settings
        self.model = os.getenv('LLM_MODEL', 'gpt-4o-mini')
        self.max_tokens = int(os.getenv('LLM_MAX_TOKENS', '1500'))
        self.temperature = float(os.getenv('LLM_TEMPERATURE', '0.3'))

    def generate_insights(
        self,
        file_path: str,
        source_name: str,
        max_sample_rows: int = 200
    ) -> Dict[str, Any]:
        """Generate insights from a data file.

        Args:
            file_path: Path to the processed data file (CSV or Parquet)
            source_name: Name of the data source
            max_sample_rows: Maximum rows to sample for analysis

        Returns:
            Dictionary with 'summary', 'insights', and 'records_analyzed'
        """
        logger.info(f"Generating insights for {source_name} from {file_path}")

        try:
            # Read and analyze the data
            df, file_type = self._read_data_file(file_path)

            if df is None or df.empty:
                logger.warning(f"No data found in {file_path}")
                return {
                    'summary': f"No data available for {source_name}.",
                    'insights': ["No data to analyze."],
                    'records_analyzed': 0
                }

            total_records = len(df)

            # Sample data for analysis
            sample_df = df.head(max_sample_rows)

            # Build context for LLM
            data_context = self._build_data_context(sample_df, total_records, source_name)

            # Generate insights using LLM
            result = self._call_llm(data_context, source_name)

            result['records_analyzed'] = total_records
            return result

        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return {
                'summary': f"Analysis of {source_name} data.",
                'insights': [f"Unable to generate detailed insights: {str(e)}"],
                'records_analyzed': 0
            }

    def _read_data_file(self, file_path: str) -> Tuple[Optional[pd.DataFrame], str]:
        """Read data from CSV or Parquet file.

        Args:
            file_path: Path to the data file

        Returns:
            Tuple of (DataFrame, file_type)
        """
        path = Path(file_path)

        if not path.exists():
            logger.error(f"File not found: {file_path}")
            return None, ""

        try:
            if path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, nrows=1000)  # Limit for large files
                return df, 'csv'
            elif path.suffix.lower() == '.parquet':
                df = pd.read_parquet(file_path)
                if len(df) > 1000:
                    df = df.head(1000)
                return df, 'parquet'
            else:
                logger.error(f"Unsupported file type: {path.suffix}")
                return None, ""
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return None, ""

    def _build_data_context(
        self,
        df: pd.DataFrame,
        total_records: int,
        source_name: str
    ) -> str:
        """Build context string for LLM analysis.

        Args:
            df: Sample DataFrame
            total_records: Total number of records in dataset
            source_name: Name of the data source

        Returns:
            Context string for LLM
        """
        # Column information
        columns_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            sample_values = df[col].dropna().head(3).tolist()

            col_info = f"- {col} ({dtype}): {non_null}/{len(df)} non-null"

            # Add statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info += f", min={df[col].min():.2f}, max={df[col].max():.2f}, mean={df[col].mean():.2f}"
            elif len(sample_values) > 0:
                col_info += f", samples: {sample_values[:3]}"

            columns_info.append(col_info)

        # Sample data (first 5 rows as JSON)
        sample_data = df.head(5).to_dict(orient='records')

        context = f"""
Data Source: {source_name}
Total Records: {total_records:,}
Columns: {len(df.columns)}

Column Details:
{chr(10).join(columns_info)}

Sample Data (first 5 rows):
{json.dumps(sample_data, indent=2, default=str)[:2000]}
"""
        return context

    def _call_llm(self, data_context: str, source_name: str) -> Dict[str, Any]:
        """Call OpenAI API to generate insights.

        Args:
            data_context: Context string with data information
            source_name: Name of the data source

        Returns:
            Dictionary with 'summary' and 'insights'
        """
        system_prompt = """You are a data analyst assistant. Analyze the provided dataset and generate:

1. A DATA SUMMARY: 2-3 sentences describing what type of data this is and its purpose.
2. KEY INSIGHTS: 3-5 bullet points highlighting important patterns, trends, anomalies, or notable findings.

Focus on business-relevant insights that would be useful for executives and decision-makers.
Be specific with numbers and percentages where possible.

Respond in this exact JSON format:
{
    "summary": "Your 2-3 sentence summary here.",
    "insights": [
        "First insight with specific data",
        "Second insight with specific data",
        "Third insight with specific data"
    ]
}"""

        user_prompt = f"""Analyze this dataset and provide a summary and key insights:

{data_context}

Remember to respond in the exact JSON format specified."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                response_format={"type": "json_object"}
            )

            # Parse the response
            content = response.choices[0].message.content
            result = json.loads(content)

            # Validate structure
            if 'summary' not in result:
                result['summary'] = f"Analysis of {source_name} data."
            if 'insights' not in result or not isinstance(result['insights'], list):
                result['insights'] = ["No specific insights generated."]

            logger.info(f"Generated {len(result['insights'])} insights for {source_name}")
            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response: {e}")
            return {
                'summary': f"Analysis of {source_name} data.",
                'insights': ["Unable to parse detailed insights."]
            }
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return {
                'summary': f"Analysis of {source_name} data.",
                'insights': [f"Analysis error: {str(e)}"]
            }


# Singleton instance for reuse
_generator_instance: Optional[InsightGenerator] = None


def get_insight_generator() -> InsightGenerator:
    """Get or create the insight generator singleton.

    Returns:
        InsightGenerator instance
    """
    global _generator_instance
    if _generator_instance is None:
        _generator_instance = InsightGenerator()
    return _generator_instance
