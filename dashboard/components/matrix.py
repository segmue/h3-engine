"""
Matrix Visualization Components.

Functions for loading and rendering association matrices.
"""

import pandas as pd

from dashboard.config import ASSOCIATION_RESULTS_PATH


def load_matrix(matrix_name: str) -> pd.DataFrame:
    """Load a matrix CSV file (semicolon-separated)."""
    filepath = ASSOCIATION_RESULTS_PATH / f"{matrix_name}_matrix.csv"
    # Try different encodings for German umlauts
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

    for encoding in encodings:
        try:
            df = pd.read_csv(filepath, sep=";", index_col=0, encoding=encoding)
            return df
        except UnicodeDecodeError:
            continue

    # Fallback: read with errors='replace'
    df = pd.read_csv(filepath, sep=";", index_col=0, encoding='utf-8', errors='replace')
    return df


def value_to_color(val) -> str:
    """Convert a value to a background color (red for negative, white for 0, blue for positive)."""
    try:
        val = float(val)
        if val < 0:
            intensity = min(abs(val), 1)
            return f"rgba(255, 100, 100, {intensity})"
        elif val > 0:
            intensity = min(val, 1)
            return f"rgba(100, 150, 255, {intensity})"
        else:
            return "white"
    except (ValueError, TypeError):
        return "white"


def compute_top5(df: pd.DataFrame) -> pd.Series:
    """Compute top 5 categories with highest values for each row (excluding self)."""
    top5_list = []
    for idx in df.index:
        row = df.loc[idx].copy()
        # Exclude self (diagonal)
        if idx in row.index:
            row = row.drop(idx)
        # Get top 5
        try:
            top5 = row.astype(float).nlargest(5).index.tolist()
            top5_list.append(", ".join(top5[:5]))
        except:
            top5_list.append("")
    return pd.Series(top5_list, index=df.index)


def render_matrix_html(df: pd.DataFrame, include_top5: bool = True) -> str:
    """Render a matrix as an HTML table with sticky headers and color coding."""
    # Check if top5 column already exists
    has_top5_col = any("5" in str(col) and "Kategor" in str(col) for col in df.columns)

    # If we need top5 but don't have it, compute it
    if include_top5 and not has_top5_col:
        top5 = compute_top5(df)

    # Get numeric columns only (exclude text columns)
    numeric_cols = []
    for col in df.columns:
        if "Anzahl" not in str(col) and "Kategor" not in str(col):
            numeric_cols.append(col)

    html = """
    <style>
        .matrix-container {
            max-height: calc(100vh - 150px);
            overflow: auto;
            position: relative;
        }
        .matrix-table {
            border-collapse: separate;
            border-spacing: 0;
            font-size: 11px;
            white-space: nowrap;
        }
        .matrix-table th, .matrix-table td {
            border: 1px solid #ddd;
            padding: 4px 6px;
            text-align: center;
            min-width: 40px;
        }
        .matrix-table thead th {
            position: sticky;
            top: 0;
            background: #f8f9fa;
            z-index: 2;
        }
        .matrix-table tbody th {
            position: sticky;
            left: 0;
            background: #f8f9fa;
            z-index: 1;
            text-align: left;
            font-weight: normal;
            max-width: 150px;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .matrix-table thead th:first-child {
            position: sticky;
            left: 0;
            z-index: 3;
        }
        .top5-col {
            position: sticky;
            right: 0;
            background: #fffde7 !important;
            z-index: 1;
            text-align: left !important;
            font-size: 10px;
            width: 300px;
            min-width: 300px;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            transition: all 0.3s ease;
            cursor: pointer;
        }
        .top5-col:hover {
            width: 450px;
            min-width: 450px;
            max-width: 450px;
            z-index: 10;
            box-shadow: -4px 0 8px rgba(0,0,0,0.15);
        }
        .matrix-table thead th.top5-col {
            z-index: 3;
        }
        .matrix-table thead th.top5-col:hover {
            z-index: 11;
        }
    </style>
    <div class="matrix-container">
        <table class="matrix-table">
            <thead>
                <tr>
                    <th></th>
    """

    # Header row
    for col in numeric_cols:
        # Truncate long names
        short_name = str(col)[:15] + "..." if len(str(col)) > 15 else str(col)
        html += f'<th title="{col}">{short_name}</th>'

    if include_top5:
        html += '<th class="top5-col">Top 5</th>'

    html += "</tr></thead><tbody>"

    # Data rows
    for i, idx in enumerate(df.index):
        html += f'<tr><th title="{idx}">{str(idx)[:20]}</th>'

        for col in numeric_cols:
            val = df.loc[idx, col]
            color = value_to_color(val)
            try:
                display_val = f"{float(val):.2f}"
            except:
                display_val = str(val)
            html += f'<td style="background:{color}">{display_val}</td>'

        if include_top5:
            if has_top5_col:
                # Find the top5 column
                top5_col = [c for c in df.columns if "5" in str(c) and "Kategor" in str(c)]
                if top5_col:
                    top5_val = df.loc[idx, top5_col[0]]
                else:
                    top5_val = ""
            else:
                top5_val = top5.loc[idx]
            # Escape quotes for title attribute
            top5_title = str(top5_val).replace('"', '&quot;')
            html += f'<td class="top5-col" title="{top5_title}">{top5_val}</td>'

        html += "</tr>"

    html += "</tbody></table></div>"
    return html
