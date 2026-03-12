"""
Spatial Association Page.

Matrix visualization of spatial association results.
"""

from shiny import Inputs, Outputs, Session, render, ui

from dashboard.config import log
from dashboard.components.matrix import load_matrix, render_matrix_html


# Page UI
association_ui = ui.tags.div(
    ui.tags.div(
        ui.input_select(
            "matrix_select",
            "Matrix:",
            {"b1": "B1 (Spatial Association)", "b2": "B2", "npmi": "NPMI"},
            selected="b1"
        ),
        ui.tags.div(
            ui.tags.span(style="display:inline-block;width:20px;height:12px;background:rgba(255,100,100,0.8);margin-right:4px;"),
            ui.tags.span("< 0 ", style="font-size:11px;margin-right:12px;"),
            ui.tags.span(style="display:inline-block;width:20px;height:12px;background:white;border:1px solid #ddd;margin-right:4px;"),
            ui.tags.span("= 0 ", style="font-size:11px;margin-right:12px;"),
            ui.tags.span(style="display:inline-block;width:20px;height:12px;background:rgba(100,150,255,0.8);margin-right:4px;"),
            ui.tags.span("> 0", style="font-size:11px;"),
            style="display:flex;align-items:center;margin-left:20px;"
        ),
        style="display:flex;align-items:center;padding:10px;"
    ),
    ui.output_ui("matrix_output"),
    style="padding:10px;"
)


def association_server(input: Inputs, output: Outputs, session: Session):
    """Server logic for Spatial Association page."""

    @render.ui
    def matrix_output():
        matrix_name = input.matrix_select()
        try:
            df = load_matrix(matrix_name)
            html = render_matrix_html(df)
            return ui.HTML(html)
        except Exception as e:
            log(f"Failed to load matrix: {e}", "error")
            return ui.tags.div(
                f"Error loading matrix: {e}",
                style="color:red;padding:20px;"
            )
