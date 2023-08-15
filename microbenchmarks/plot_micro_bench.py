import argparse
import sys

import pandas as pd
import matplotlib.pyplot as plt


def _parser_output(output: str):
    rows = []
    for line in output.splitlines():
        num_row, num_select, runtime, *_ = line.strip().split(" ")
        rows.append([int(num_row), int(num_select), float(runtime)])
    return pd.DataFrame(rows, columns=["num_row", "num_select", "runtime"])


def plot_micro_bench(df: pd.DataFrame, output_filename: str):
    # Plot num_row vs runtime in a separate axis for each num_select.
    fig, axes = plt.subplots(
        1,
        len(df.num_select.unique()),
        figsize=(25, 5),
        sharex=True,
    )
    for i, num_select in enumerate(df.num_select.unique()):
        df_group = (
            df[df.num_select == num_select].groupby(["num_row"])["runtime"].mean()
        )
        print(df_group)
        plots = df_group.plot(
            x="num_row",
            y="runtime",
            ax=axes[i],
            kind="bar",
            title="num_select={}".format(num_select),
        )
        for bar in plots.patches:
            plots.annotate(
                format(bar.get_height(), ".4f"),
                (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                ha="center",
                va="center",
                size=8,
                xytext=(0, 8),
                textcoords="offset points",
            )
        axes[i].set_yscale("log", base=10)
    plt.savefig(output_filename, bbox_inches="tight")


parser = argparse.ArgumentParser(
    description="Plot micro benchmark results reading from stdin."
)
parser.add_argument("--output", type=str, help="output filename", required=True)
args = parser.parse_args()

output = sys.stdin.read()
if output:
    df = _parser_output(output)
    plot_micro_bench(df, args.output)
else:
    print("No output found in stdin.")
    sys.exit(1)
