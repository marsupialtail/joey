import ipywidgets as widgets
from IPython.display import display
import pyjoey
from . import vector_interval_cep, nfa_cep
import json
from functools import partial
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import polars
import base64


def render(table, extra_columns_in_results = []):

    if table is None:
        print("PLEASE DEFINE THE DATA FIRST ABOVE!")

    blue_button_style = {'description_width': 'initial', 'button_color': 'lightblue'}
    red_button_style = {'description_width': 'initial', 'button_color': '#FF6666'}
    text_style = {'description_width': 'initial'}
    text_layout = widgets.Layout(width='1000px', 
                                border='2px solid black', 
                                font_size='20px')  # Adjust font size as needed

    # Create a custom button layout
    button_layout = widgets.Layout(width='300px', 
                                height='40px', 
                                border='2px solid black', 
                                margin='5px', 
                                padding='5px')
    box_layout = widgets.Layout(display='flex', flex_flow='row',  align_items='stretch',  width='70%')
    
    event_names = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    text_inputs = []
    
    prompt_space = widgets.Output()
    output = widgets.Output()
    output1 = widgets.Output()
    download_link = widgets.HTML(layout = button_layout)
    debug_result = None
    result = None
    
    ts_widget_description = widgets.HTML(
        value="<b>Timestamp column:</b><br> Must be integer datatype.",
    )
    
    json_condition_area = widgets.Textarea(
        value='Your text here',
        placeholder='Type something',
        description='Text:',
        disabled=False,
        layout=widgets.Layout(height='300px', width='auto')  # Set the height here
    )
    
    ts_widget = widgets.Dropdown(
        options = [i for i, k in table.schema.items() if k in polars.INTEGER_DTYPES],
        description = '',
        disabled = False,
        style = {'description_width': 'initial'},
        layout = widgets.Layout(width='100px')
    )
    
    groupby_widget_description = widgets.HTML(
        value="<b>Grouping column(s):</b><br> Patterns will be found for each group.",
    )
    
    groupby_widget = widgets.SelectMultiple(
        options= table.columns,
        # value=[],  # Initially selected values (empty list for none selected)
        description='',
        disabled=False,  # Set to True to make it read-only
        width = 600,
        style = {'description_width': 'initial'},
        layout = widgets.Layout(overflow_y="visible",width='400px', height='200px', max_height='250px')
    )
    
    limit_widget_description = widgets.HTML(
        value="<b>Pattern length limit:</b><br> Timestamp of last event - first event must be smaller than this.<br> Analysis might be slow if this number is too big.",
    )
    
    limit_widget = widgets.Text(
        value='',  # Initial value
        placeholder='Type a number',  # Placeholder text
        description='Limit:',  # Label for the text input
    )
    
    def create_text_input(b):
        text_input = widgets.Text(description='Event ' + event_names[len(text_inputs)], placeholder='Type SQL predicate.', style=text_style, layout=text_layout)
        text_inputs.append(text_input)
        with output:
            display(text_input)
    
    def remove_text_input(b):
        if text_inputs:
            text_inputs.pop()
            with output:
                output.clear_output()
                for text_input in text_inputs:
                    display(text_input)
    
    def display_json_input(b):
        # clear the text inputs
        text_inputs = []
        json_input = widgets.Text(description='Conditions JSON: ', placeholder='Type SQL predicate.', style=text_style, layout=text_layout)
        with output:
            output.clear_output()
            display(json_input)
    
    def create_download_link(df, title = "Download CSV result", filename = "data.csv"):
        
        csv = df.to_pandas().to_csv()
        b64 = base64.b64encode(csv.encode())
        payload = b64.decode()
        html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
        html = html.format(payload=payload,title=title,filename=filename)
        return html
    
    def create_result(mode, b):
    
        global result, debug_result
    
        groupby_cols = list(groupby_widget.value)
        try:
            max_span = int(limit_widget.value)
        except:
            max_span = 2 ** 31 - 1
        
        if mode == 'text':
            conditions = [(event_names[i],k.value) for i,k in enumerate(text_inputs)]
        elif mode == 'json':
            conditions = json.loads(json_condition_area.value)
        
        ts_column = ts_widget.value
        result = vector_interval_cep(table, conditions, ts_column, max_span, by = groupby_cols, fix = 'start')
        debug_result = result

        select_columns = list(set(groupby_cols + [ts_column] + extra_columns_in_results +
                     list(pyjoey.utils.touched_columns(conditions))))
        
        if result is not None:
            result = result.groupby(groupby_cols + [event_names[len(conditions) -1] + '_' + ts_column]).agg(polars.col("a_" + ts_column).min())\
                .melt(id_vars = groupby_cols, value_vars = [f"a_{ts_column}", f"{event_names[len(conditions) -1]}_{ts_column}"])\
                .join(table.select(select_columns), left_on = groupby_cols + ['value'], right_on = groupby_cols + [ts_column])\
                .sort(groupby_cols + ['value'])
    
        with output1:
            output1.clear_output()
            if result is not None:
                download_link.value = create_download_link(result)
                print("Performed analysis on {} groups, event occurred in {} groups".format(len(table.unique(groupby_cols)) ,len(result.unique(groupby_cols)) ))
                x = result.groupby(groupby_cols).agg(polars.col("value"))\
                    .sort(groupby_cols)\
                    .select([polars.concat_list(groupby_cols), "value"])\
                    .map_rows(lambda x: (x[0], [x[1][i:i + 2] for i in range(0, len(x[1]), 2)])).to_dicts()
                events = {str(i['column_0']) : i['column_1'] for i in x}
                
                # Example data using integer timestamps (e.g., hours)
                categories = events.keys()
                
                fig, ax = plt.subplots(figsize=(20,30))
                
                # Height of each box
                box_height = 0.4
                
                # Create a box for each event
                for i, category in enumerate(categories):
                    [ax.add_patch(Rectangle((start, i - box_height / 2), end - start, box_height, color='blue', alpha=0.5)) for start,end in events[category]]
                
                ax.set_yticks(range(len(categories)))
                ax.set_yticklabels(categories)
                ax.set_xlim(0, result["value"].max() * 1.1)  
                
                # Set labels and title
                plt.xlabel('Measurement Number')
                plt.ylabel(str(groupby_cols))
                plt.title('Events Timeline')
                
                plt.show()
            else:
                print("No events found.")
    
    def print_json(b):
        with output1:
            print(json.dumps([(event_names[i],k.value) for i,k in enumerate(text_inputs)]))
    
    
    add_button = widgets.Button(description="Add a new condition.", style=blue_button_style, layout=button_layout)
    remove_button = widgets.Button(description="Remove the last condition.", style=blue_button_style, layout=button_layout)
    compute_button = widgets.Button(description = "Compute", style=red_button_style, layout=button_layout)
    compute_button_json = widgets.Button(description = "Compute", style=red_button_style, layout=button_layout)
    
    serialize_button = widgets.Button(description = "Get event in Json format",style=blue_button_style, layout=button_layout )
    add_button.on_click(create_text_input)
    remove_button.on_click(remove_text_input)
    
    compute_button.on_click(partial(create_result, 'text'))
    compute_button_json.on_click(partial(create_result, 'json'))
    
    serialize_button.on_click(print_json)
    
    # Display the button and the output widget
    
    def replace_prompt_space(b):
        output.clear_output()
        output1.clear_output()
        with prompt_space:
            text_inputs.clear()
            prompt_space.clear_output()
            if not toggle_button.value:
                display(widgets.HBox([add_button, remove_button]), output, widgets.HBox([compute_button, serialize_button]), output1)
            else:
                display(json_condition_area, widgets.HBox([compute_button_json]), output1)
    
    toggle_button = widgets.ToggleButton(
        value=False,
        description='Toggle Visual Builder / Code View',
        button_style='', # 'success', 'info', 'warning', 'danger' or ''
        tooltip='Toggle View',
        icon='circle', # icons are from Font Awesome,
        layout = button_layout
    )
    toggle_button.observe(replace_prompt_space)
    spacer = widgets.Label(' ', layout=widgets.Layout(width='10px'))
    
    title = widgets.HTML(
        value="<h2 style='background-color:blue; color:white; text-align:center; padding:10px;'>Second, define the pattern</h2>",
    )

    display(title, widgets.HBox([widgets.VBox([ts_widget_description, ts_widget]), spacer, widgets.VBox([groupby_widget_description, groupby_widget]), spacer, widgets.VBox([limit_widget_description, limit_widget])]), widgets.HBox([toggle_button, download_link]), prompt_space)
    replace_prompt_space(True)