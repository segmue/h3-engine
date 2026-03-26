"""
Sentence Generator Tab - Dashboard Page.

Ermoeglicht das Suchen von Features und Generieren von Candidate Sentences
basierend auf der B1 Association Matrix und H3 Spatial Intersection.
"""

from shiny import Inputs, Outputs, Session, ui, render, reactive

from engine import H3Engine
from sentence_generator import CandidateSentenceGenerator, FeatureInput, SentenceGeneratorConfig
from dashboard.config import DB_PATH


# Engine und Generator initialisieren (einmal pro Modul-Load)
engine = H3Engine(DB_PATH)
config = SentenceGeneratorConfig.from_config_yaml()
generator = CandidateSentenceGenerator(engine, config)


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------

sentence_gen_ui = ui.page_sidebar(
    ui.sidebar(
        ui.h5("Feature suchen"),
        ui.input_text("sg_search_name", "Name", placeholder="z.B. Matterhorn"),
        ui.input_action_button("sg_search_btn", "Suchen", class_="btn-primary btn-sm"),
        ui.hr(style="margin: 10px 0;"),
        ui.output_ui("sg_search_results"),
        ui.hr(style="margin: 10px 0;"),
        ui.input_action_button(
            "sg_generate_btn",
            "Satz generieren",
            class_="btn-success"
        ),
        width=300
    ),
    # Main content
    ui.output_ui("sg_main_content"),
    title="Sentence Generator"
)


# -----------------------------------------------------------------------------
# Server
# -----------------------------------------------------------------------------

def sentence_gen_server(input: Inputs, output: Outputs, session: Session):
    """Server-Logik fuer den Sentence Generator Tab."""

    # Reactive state
    found_features = reactive.Value([])
    generation_result = reactive.Value(None)

    # -------------------------------------------------------------------------
    # Search
    # -------------------------------------------------------------------------

    @reactive.Effect
    @reactive.event(input.sg_search_btn)
    def do_search():
        name = input.sg_search_name()
        if not name or len(name) < 2:
            found_features.set([])
            return

        # SQL-Injection verhindern durch Escaping
        safe_name = name.replace("'", "''")

        results = engine.conn.execute(f"""
            SELECT feature_id, NAME, OBJEKTART
            FROM features
            WHERE NAME ILIKE '%{safe_name}%'
            ORDER BY NAME
            LIMIT 30
        """).fetchall()

        found_features.set(results)

    @render.ui
    def sg_search_results():
        features = found_features.get()

        if not features:
            return ui.p("Keine Ergebnisse", style="color: #666; font-size: 0.9em;")

        # Radio buttons fuer Auswahl
        choices = {str(f[0]): f"{f[1]} ({f[2]})" for f in features}
        return ui.div(
            ui.p(f"{len(features)} Treffer:", style="font-size: 0.85em; margin-bottom: 5px;"),
            ui.input_radio_buttons(
                "sg_feature_select",
                None,
                choices,
                width="100%"
            ),
            style="max-height: 300px; overflow-y: auto;"
        )

    # -------------------------------------------------------------------------
    # Generate
    # -------------------------------------------------------------------------

    @reactive.Effect
    @reactive.event(input.sg_generate_btn)
    def do_generate():
        feature_id = input.sg_feature_select()
        if not feature_id:
            return

        # Feature-Details holen
        row = engine.conn.execute(f"""
            SELECT feature_id, NAME, OBJEKTART
            FROM features
            WHERE feature_id = {feature_id}
        """).fetchone()

        if not row:
            return

        # Sentence generieren
        feature = FeatureInput(feature_id=row[0], name=row[1], objektart=row[2])
        result = generator.generate(feature)

        # Auch die Slot-Allokation fuer die Tabelle holen
        associated = generator._assoc_loader.get_associated_categories(
            row[2],
            generator.config.assoc_threshold,
            generator.config.max_categories
        )
        slots = generator._allocate_slots(associated)

        generation_result.set({
            "feature": row,
            "associated": associated,
            "slots": slots,
            "result": result
        })

    # -------------------------------------------------------------------------
    # Main Content
    # -------------------------------------------------------------------------

    @render.ui
    def sg_main_content():
        data = generation_result.get()

        if not data:
            return ui.div(
                ui.h4("Sentence Generator"),
                ui.p(
                    "Suche ein Feature in der Sidebar und klicke 'Satz generieren' "
                    "um einen Candidate Sentence zu erstellen."
                ),
                ui.hr(),
                ui.p(
                    "Der Generator verwendet die B1 Association Matrix um relevante "
                    "Kategorien zu identifizieren und findet dann via H3 Spatial Intersection "
                    "konkrete Instanzen in der Naehe.",
                    style="color: #666;"
                ),
                style="padding: 20px;"
            )

        feature = data["feature"]
        associated = data["associated"]
        slots = data["slots"]
        result = data["result"]

        # Feature-Info Card
        feature_card = ui.card(
            ui.card_header("Feature"),
            ui.tags.table(
                ui.tags.tr(ui.tags.td("Name:"), ui.tags.td(feature[1], style="font-weight:bold;")),
                ui.tags.tr(ui.tags.td("OBJEKTART:"), ui.tags.td(feature[2])),
                ui.tags.tr(ui.tags.td("Feature ID:"), ui.tags.td(str(feature[0]))),
                class_="table table-sm",
                style="margin-bottom: 0;"
            )
        )

        # Assoziations-Tabelle
        if associated:
            assoc_rows = [
                ui.tags.tr(
                    ui.tags.td(cat),
                    ui.tags.td(f"{b1:.4f}"),
                    ui.tags.td(str(slots.get(cat, 0)), style="text-align:center;")
                )
                for cat, b1 in associated
            ]
            assoc_table = ui.tags.table(
                ui.tags.thead(ui.tags.tr(
                    ui.tags.th("Kategorie"),
                    ui.tags.th("B1 Wert"),
                    ui.tags.th("Slots", style="text-align:center;")
                )),
                ui.tags.tbody(*assoc_rows),
                class_="table table-sm table-striped"
            )
        else:
            assoc_table = ui.p(
                "Keine assoziierten Kategorien gefunden (B1 > threshold)",
                style="color: #999;"
            )

        # Gefundene Instanzen Tabelle
        if result.context_by_category:
            context_rows = []
            for cat, names in result.context_by_category.items():
                context_rows.append(ui.tags.tr(
                    ui.tags.td(cat),
                    ui.tags.td(", ".join(names) if names else "-")
                ))
            context_table = ui.tags.table(
                ui.tags.thead(ui.tags.tr(
                    ui.tags.th("Kategorie"),
                    ui.tags.th("Gefundene Instanzen")
                )),
                ui.tags.tbody(*context_rows),
                class_="table table-sm table-striped"
            )
        else:
            context_table = ui.p(
                "Keine intersecting Instanzen mit Namen gefunden",
                style="color: #999;"
            )

        # Candidate Sentence Card
        sentence_card = ui.card(
            ui.card_header("Candidate Sentence"),
            ui.p(
                result.sentence,
                style="font-size: 1.15em; font-weight: bold; padding: 10px; "
                      "background: #f8f9fa; border-radius: 4px;"
            )
        )

        return ui.div(
            feature_card,
            ui.h5("Assoziierte Kategorien (B1 Matrix)", style="margin-top: 25px;"),
            assoc_table,
            ui.h5("Gefundene Instanzen", style="margin-top: 25px;"),
            context_table,
            ui.div(style="margin-top: 25px;"),
            sentence_card,
            style="padding: 20px; max-width: 800px;"
        )
