import ipywidgets as widgets
from IPython.display import display
import pyjoey
from . import nfa_interval_cep, vector_interval_cep, nfa_cep
import json
from functools import partial, reduce
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import polars
import base64

blue_button_style = {"description_width": "initial", "button_color": "lightblue"}
red_button_style = {"description_width": "initial", "button_color": "#FF6666"}
text_style = {"description_width": "initial"}
text_layout = widgets.Layout(
    width="1000px", border="2px solid black", font_size="20px"
)  # Adjust font size as needed

# Create a custom button layout
button_layout = widgets.Layout(
    width="300px",
    height="40px",
    border="2px solid black",
    margin="5px",
    padding="5px",
)
event_names = [i for i in 'abcdefghijklmnopqrstuvwxyz']
spacer = widgets.Label(" ", layout=widgets.Layout(width="10px"))

def create_download_link(df, title="Download CSV result", filename="data.csv"):
    csv = df.to_pandas().to_csv()
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload, title=title, filename=filename)
    return html

def display_timeline(table, result, groupby_cols, ts_column, conditions):
    # if you have some grouping columns, show some stats on the groups
    if len(groupby_cols) > 0:
        print(
            "Performed analysis on {} groups, event occurred in {} groups".format(
                len(table.unique(groupby_cols)) ,
                len(result.unique(groupby_cols)) ,
            )
        )

        x = (
            result.groupby(groupby_cols)
            .agg(polars.col("value"))
            .sort(groupby_cols)
            .select([polars.concat_list(groupby_cols), "value"])
            .map_rows(
                lambda x: (
                    x[0],
                    [
                        x[1][i :: (len(x[1]) // len(conditions))]
                        for i in range(0, len(x[1]) // len(conditions))
                    ],
                )
            )
            .to_dicts()
        )

        events = {json.dumps(i["column_0"]): i["column_1"] for i in x}

    else:

        events = {json.dumps("events"): [
                    result['value'][i :: (len(result) // len(conditions))]
                    for i in range(0, len(result) // len(conditions))
                ]}

    # Example data using integer timestamps (e.g., hours)
    categories = list(events.keys())

    fig, ax = plt.subplots(figsize=(20, max(len(result) // 5, 5)))

    # Height of each box
    box_height = 0.4

    event_lengths = []
    
    for i, category in enumerate(categories):

        # Create a box for each matched sequence for this group
        for ts_list in events[category]:
            start, end = ts_list[0], ts_list[-1]
            ax.add_patch(
                Rectangle(
                    (start, i - box_height / 2),
                    end - start,
                    box_height,
                    color="blue",
                    alpha=0.5,
                )
            )
            event_lengths.append(end - start)

            # Draw a line for each event
            for ts in ts_list[1:-1]:
                ax.vlines(
                    x=ts,
                    ymin=i - box_height / 2,
                    ymax=i + box_height / 2,
                    color="red",
                    linestyle="--",
                )

    longest_event = max(event_lengths)
    ax.set_yticks(range(len(categories)))
    ax.set_yticklabels(categories)
    ax.set_xlim(
        max(result["value"].min() - longest_event, 0),
        result["value"].max() + longest_event,
    )

    # Set labels and title
    plt.xlabel(ts_column)
    plt.ylabel(str(groupby_cols))
    plt.title("Events Timeline")

    plt.show()
    return events, categories

def render_builder(table, text_inputs, condition_list_space, output_space):
    ts_widget_description = widgets.HTML(
        value="<b>Timestamp column:</b><br> Must be integer datatype.",
    )

    ts_widget = widgets.Dropdown(
        options=[i for i, k in table.schema.items() if k in polars.INTEGER_DTYPES],
        description="",
        disabled=False,
        style={"description_width": "initial"},
        layout=widgets.Layout(width="100px"),
    )

    groupby_widget_description = widgets.HTML(
        value="<b>Grouping column(s):</b><br> Patterns will be found for each group.",
    )

    groupby_widget = widgets.SelectMultiple(
        options=table.columns,
        # value=[],  # Initially selected values (empty list for none selected)
        description="",
        disabled=False,  # Set to True to make it read-only
        width=600,
        style={"description_width": "initial"},
        layout=widgets.Layout(
            overflow_y="visible", width="400px", height="200px", max_height="250px"
        ),
    )

    limit_widget_description = widgets.HTML(
        value="""<b>Pattern length limit:</b><br> Timestamp of last event - first event must be smaller than this.
        <br> Analysis might be slow if this number is too big.
        <br> This uses the same unit as your integer timestamp column.""",
    )

    limit_widget = widgets.Text(
        value="30",  # Initial value
        placeholder="Type a number",  # Placeholder text
        description="Limit:",  # Label for the text input
    )

    def create_text_input(b):
        text_input = widgets.Text(
            description="Event " + event_names[len(text_inputs)],
            placeholder="Type SQL predicate.",
            style=text_style,
            layout=text_layout,
        )
        text_inputs.append(text_input)
        with condition_list_space:
            display(text_input)

    def remove_text_input(b):
        if text_inputs:
            text_inputs.pop()
            with condition_list_space:
                condition_list_space.clear_output()
                for text_input in text_inputs:
                    display(text_input)
    def print_json(b):
        with output_space:
            output_space.clear_output()
            print("Serialized event (paste directly into Code View):")
            print(
                json.dumps(
                    {
                        "ts_column": ts_widget.value,
                        "by": list(groupby_widget.value),
                        "max_span": int(limit_widget.value),
                        "conditions": [
                            (event_names[i], k.value) for i, k in enumerate(text_inputs)
                        ],
                    }
                )
            )

    add_button = widgets.Button(
        description="Add a new condition.",
        style=blue_button_style,
        layout=button_layout,
    )
    remove_button = widgets.Button(
        description="Remove the last condition.",
        style=blue_button_style,
        layout=button_layout,
    )
    compute_button = widgets.Button(
        description="Compute", style=red_button_style, layout=button_layout
    )

    serialize_button = widgets.Button(
        description="Get event in Json format",
        style=blue_button_style,
        layout=button_layout,
    )
    add_button.on_click(create_text_input)
    remove_button.on_click(remove_text_input)
    serialize_button.on_click(print_json)

    return {
        "ts_widget_description": ts_widget_description,
        "ts_widget": ts_widget,
        "groupby_widget_description": groupby_widget_description,
        "groupby_widget": groupby_widget,
        "limit_widget_description": limit_widget_description,
        "limit_widget": limit_widget,
        "add_button": add_button,
        "remove_button": remove_button,
        "compute_button": compute_button,
        "serialize_button": serialize_button,
    }

def display_event(table, events, groupby_cols, ts_column, conditions, event):
    
    fig, ax = plt.subplots(figsize=(20, 10))

    event_list = json.loads(event)
    filtered_table = table.filter(
        reduce(
            lambda x, y: x & y,
            [
                polars.col(col) == val
                for col, val in zip(groupby_cols, event_list)
            ],
        )
    ) if len(groupby_cols) > 0 else table
    plot_cols = [
        col
        for col in list(pyjoey.utils.touched_columns(conditions))
        if (
            col != ts_column
            and col not in groupby_cols
            and filtered_table[col].dtype in polars.NUMERIC_DTYPES
        )
    ]

    for col in plot_cols:
        ax.plot(
            filtered_table[ts_column],
            filtered_table[col],
            label=col,
        )
    ax.legend()
    event_colors = [
        "red",
        "green",
        "blue",
        "yellow",
        "orange",
        "purple",
        "pink",
        "brown",
        "black",
    ]
    min_ts = min([i[0] for i in events[event]])
    max_ts = max([i[-1] for i in events[event]])
    max_length = max([i[-1] - i[0] for i in events[event]])
    for i, ts_list in enumerate(events[event]):
        my_color = event_colors[i % len(event_colors)]
        for ts in ts_list:
            ax.axvline(x=ts, color=my_color, linestyle="--")
    
    ax.set_xlim(
        max(min_ts - max_length, filtered_table[ts_column].min()),
        min(max_ts + max_length, filtered_table[ts_column].max()),
    )
    plt.xlabel("ts_column")
    plt.ylabel("Value")
    plt.show()

def render(table, extra_columns_in_results=[]):

    if table is None:
        print("PLEASE DEFINE THE DATA FIRST!")
        return
    
    text_inputs = []
    
    json_condition_area = widgets.Textarea(
        value="Your text here",
        placeholder="Type something",
        description="Text:",
        disabled=False,
        layout=widgets.Layout(height="300px", width="auto"),  # Set the height here
    )

    compute_button_json = widgets.Button(
        description="Compute", style=red_button_style, layout=button_layout
    )
    
    prompt_space = widgets.Output()
    condition_list_space = widgets.Output()
    output_space = widgets.Output()
    download_link = widgets.HTML(layout=button_layout)

    builder_widgets = render_builder(table, text_inputs, condition_list_space, output_space)

    def create_result(mode, b):
        if mode == "text":
            conditions = [(event_names[i], k.value) for i, k in enumerate(text_inputs)]
            groupby_cols = list(builder_widgets["groupby_widget"].value)
            ts_column = builder_widgets["ts_widget"].value
            try:
                max_span = int(builder_widgets["limit_widget"].value)
            except:
                max_span = 2**31 - 1
        elif mode == "json":
            data = json.loads(json_condition_area.value)
            conditions = data["conditions"]
            ts_column = data["ts_column"]
            groupby_cols = data["by"] if "by" in data else []
            max_span = data["max_span"] if "max_span" in data else 2**31 - 1

        with output_space:
            output_space.clear_output()
            print(f"Too slow? Make sure you are on the latest pyjoey version: https://pypi.org/project/pyjoey/. Your current version {pyjoey.__version__}")
            print("Make an issue report on Github: https://github.com/marsupialtail/joey.")
            print("Describe in your issue the number of groups in your data and the expected number of results.")
            
            # table = table.sort(groupby_cols + [ts_column])
            result = nfa_interval_cep(
                table,
                conditions,
                ts_column,
                max_span,
                by=groupby_cols if len(groupby_cols) > 0 else None,
                fix="start",
            )
            if result is None:
                print("No events found.")
                return

            # groupby_col will be a list here.

            select_columns = list(
                set(
                    groupby_cols
                    + [ts_column]
                    + extra_columns_in_results
                    + list(pyjoey.utils.touched_columns(conditions))
                )
            )

            # result will have columns like a_timestamp, b_timestamp, c_timestamp, we have to melt it to join with the original table

            result = (
                result.groupby(
                    groupby_cols + [event_names[len(conditions) - 1] + "_" + ts_column]
                )
                .agg(
                    [
                        polars.col(event_names[i] + "_" + ts_column).min()
                        for i in range(len(conditions) - 1)
                    ]
                )
                .melt(
                    id_vars=groupby_cols,
                    value_vars=[
                        f"{event_names[i]}_{ts_column}" for i in range(len(conditions))
                    ],
                )
                .join(
                    table.select(select_columns),
                    left_on=groupby_cols + ["value"],
                    right_on=groupby_cols + [ts_column],
                )
                .sort(groupby_cols + ["variable"])
            )

            download_link.value = create_download_link(result)
            events, categories = display_timeline(table, result, groupby_cols, ts_column, conditions)

            title = widgets.HTML(
                value="<h2 style='background-color:blue; color:white; text-align:center; padding:10px;'>Analyze single event</h2>",
            )
            event_selector = widgets.Dropdown(
                options=['None'] + categories,
                description="Select event",
                disabled=False,
                style={"description_width": "initial"},
                layout=widgets.Layout(overflow_y="visible"),
            )
            event_output = widgets.Output()

            def display_event_callback(b):
                if event_selector.value == 'None':
                    return
                with event_output:
                    event_output.clear_output()
                    display_event(table, events, groupby_cols, ts_column, conditions, event_selector.value)

            event_selector.observe(display_event_callback, names='value')
            display(title, event_selector, event_output)

    builder_widgets["compute_button"].on_click(partial(create_result, "text"))
    compute_button_json.on_click(partial(create_result, "json"))

    # Display the button and the output widget

    def replace_prompt_space(b):
        condition_list_space.clear_output()
        output_space.clear_output()
        with prompt_space:
            text_inputs.clear()
            prompt_space.clear_output()
            if not toggle_button.value:
                display(
                    widgets.HBox(
                        [
                            widgets.VBox([builder_widgets["ts_widget_description"], builder_widgets["ts_widget"]]),
                            spacer,
                            widgets.VBox([builder_widgets["groupby_widget_description"], builder_widgets["groupby_widget"]]),
                            spacer,
                            widgets.VBox([builder_widgets["limit_widget_description"], builder_widgets["limit_widget"]]),
                        ]
                    ),
                    widgets.HBox([builder_widgets["add_button"], builder_widgets["remove_button"]]),
                    condition_list_space,
                    widgets.HBox([builder_widgets["compute_button"], builder_widgets["serialize_button"]]),
                    output_space,
                )
            else:
                display(
                    json_condition_area,
                    widgets.HBox([compute_button_json]),
                    output_space,
                )

    toggle_button = widgets.ToggleButton(
        value=False,
        description="Toggle Visual Builder / Code View",
        button_style="",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip="Toggle View",
        icon="circle",  # icons are from Font Awesome,
        layout=button_layout,
    )
    toggle_button.observe(replace_prompt_space)

    title = widgets.HTML(
        value="<h2 style='background-color:blue; color:white; text-align:center; padding:10px;'>Define the pattern</h2>",
    )

    display(title, widgets.HBox([toggle_button, download_link]), prompt_space)
    replace_prompt_space(True)
