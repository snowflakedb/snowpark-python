from __future__ import annotations

import json
import subprocess
import webbrowser
import signal
from collections import defaultdict
from pathlib import Path
from typing import Callable, DefaultDict, Dict, List, Tuple, Type
import atexit

import networkx as nx
import pygments
from networkx import DiGraph
from networkx.readwrite import json_graph
from pygments.lexers import SqlLexer
import sys
from pygments.token import Punctuation, Whitespace
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.relational_grouped_dataframe import RelationalGroupedDataFrame
from pygraphviz import AGraph


SQL_INDENT = 2
"""
Using space = 2 for SQL queries.
"""

# Try coloring the node if colorcet is installed
try:
    import colorcet as cc

    COLOR_SEQ = cc.glasbey_light
except ModuleNotFoundError:
    COLOR_SEQ = ["#FFFFFF"]


def debug_vis(root: DataFrame, output_folder: Path, browser: bool = True):
    """
    Debugger and visualizer from the given root.
    Dumps the results into the target output directory.
    """

    output_folder.mkdir(parents=True, exist_ok=True)

    # Don't pollute the git project.
    (output_folder / ".gitignore").write_text("# Auto generated\n*\n")

    graph, visited = _visit_nodes(root)

    color = _color_palatte(graph=graph, visited=visited)

    # Layout
    pos: Dict[str, Tuple[float, float]] = nx.nx_agraph.graphviz_layout(graph)
    for name in graph.nodes:
        x, y = pos[name]
        graph.nodes[name]["pos"] = f"{x},{y}!"
        graph.nodes[name]["layout"] = x, y
        graph.nodes[name]["fillcolor"] = color[name]
        graph.nodes[name]["style"] = "filled"

    # Store SQL
    for node, name in visited.items():
        sql = node.generate_sql()

        (output_folder / f"{name}.sql").write_text(sql)
        graph.nodes[name]["raw_sql"] = sql
        graph.nodes[name]["extras"] = dict(node.depends_on().extras.items())

    # Color SQL
    for name, colored in _sql_highlighter(graph=graph, visited=visited).items():
        graph.nodes[name]["sql"] = colored

    # Generate graphviz files.
    agraph: AGraph = nx.drawing.nx_agraph.to_agraph(graph)
    agraph.write(output_folder / "query_tree.dot")

    # Generate json for D3
    graph_json = json_graph.node_link_data(graph)
    with open(output_folder / "query_tree.json", "w+") as f:
        json.dump(graph_json, f)

    html = output_folder / "index.html"
    html.write_text(_TEMPLATE)

    if browser:
        # I'm lazy to configure python http server, so subprocess.
        # Open browser after it is configured.
        p = subprocess.Popen(
            ["python3", "-m", "http.server", "-d", output_folder, "3777"],
        )
        atexit.register(lambda : p.kill())
        webbrowser.open("http://localhost:3777")


def _visit_nodes(
    root: DataFrame,
) -> Tuple[DiGraph, Dict[DataFrame, str]]:
    """
    Visit the ancestor for each node, assign each a name,
    and return the graph and name mapping.

    Parameters
    ----------

    root: DataFrame
        The root to visit.
    """

    # Mapping from each visited node to their assigned names.
    # Names are assigned on first visit.
    visited: Dict[DataFrame, str] = {}

    # Count occurences of each type.
    # Coupled with visited, to distinguish between nodes when assigning name.
    node_type_count: DefaultDict[Type[DataFrame], int] = defaultdict(int)

    # A graph of names. Storing names for easy visualization.
    graph = DiGraph()

    _recursive_visit(
        root, visited=visited, node_type_count=node_type_count, graph=graph
    )

    return graph, visited

def _get_node_type(node):
    try:
        return node.depends_on().extras['operation']
    except KeyError:
        return node.depends_on().extras['Operation']


def _recursive_visit(
    root: DataFrame,
    *,
    visited: Dict[DataFrame, str],
    node_type_count: Dict[Type[DataFrame], int],
    graph: DiGraph,
    context: DataFrame | None = None,
):
    """
    Parameters
    ----------

    root: DataFrame
        The youngest descendant of the current graph.

    context: DataFrame | None
        The child of the current subtree. None for the starting node.
    """

    assert isinstance(root, (DataFrame, RelationalGroupedDataFrame)), type(root)

    if root in visited:
        return

    # Mark visited
    node_type_count[_get_node_type(root)] += 1
    assert root not in visited
    root_name = f"{_get_node_type(root)}_{node_type_count[_get_node_type(root)]}"
    visited[root] = root_name

    assert root_name not in graph.nodes
    # Register context
    if context is not None:
        graph.add_node(root_name)
        graph.add_edge(root_name, visited[context])

    # Sanity checks.
    assert root in visited

    for par in root.depends_on().parents:
        if par in visited:
            graph.add_edge(visited[par], root_name)
        else:
            _recursive_visit(
                par,
                visited=visited,
                node_type_count=node_type_count,
                graph=graph,
                context=root,
            )

    # Sanity checks.
    for par in root.depends_on().parents:
        assert par in visited


def remove_whitespace(sql: str):
    # Using lexers instead of parsers because
    # most parsers I tried don't work with partial SQL.
    # Also I think pygments is already a dependency of jupyter, common enough.
    lexed = pygments.lex(sql, SqlLexer())
    removed = [[typ, tok] for typ, tok in lexed if typ != Whitespace]

    # Append whitespace wherever appropriate.
    # These rules are to handle parenthesis,
    # s.t. it is formatted as (a, b, c) rather than ( a , b , c )
    result = []
    for typ, tok in removed:
        # No whitespace before , )
        if typ == Punctuation and tok in [",", ")"]:
            if len(result) and result[-1][0] == Whitespace:
                result.pop()

        result.append([typ, tok])

        # No whitespace after (
        if not (typ == Punctuation and tok in ["("]):
            result.append([Whitespace, " "])

    # Only need tokens, remove final whitespace.
    return "".join([r[1] for r in result]).strip()


def _split_by_generated_sql(node: DataFrame) -> List[str | DataFrame]:
    """
    Replace the generate_sql() calls for their generator.

    E.g.
    If a node's generate_sql() is generated by
    `f"SELECT a, b, c FROM ({other.generate_sql()})"`,
    then this would return: ["SELECT a, b, c FROM (", other, ")"]
    """

    sql = node.generate_sql()

    # For each iteration, split SQL and then
    current_split: List[str | DataFrame] = [sql]
    for parent in node.depends_on().parents:
        next_split = []
        sub_sql = parent.generate_sql()

        for sql_excerpt in current_split:
            if isinstance(sql_excerpt, DataFrame):
                next_split.append(sql_excerpt)
                continue

            assert isinstance(sql_excerpt, str)

            # Remove all whitespaces
            splitted = [remove_whitespace(s) for s in sql_excerpt.split(sub_sql)]

            # Do a [parent].join(splitted)
            joined = []
            for split in splitted:
                joined.extend([split, parent])
            joined = joined[:-1]
            next_split.extend(joined)

        assert len(current_split) <= len(next_split), {
            "current": current_split,
            "next": next_split,
        }
        current_split = next_split

    return current_split


def _colorize(line: str, color: str) -> str:
    """
    Colorize the background color of a line in html.
    """

    assert color.startswith("#"), color
    return f"""<span style="background-color: {color}">{line}</span>"""


def _recursive_gen_sql_indent(
    root: DataFrame,
    *,
    sql_splits: Dict[DataFrame, List[str | DataFrame]],
    indent_level: int = 0,
    background: Callable[[DataFrame], str] = lambda _: "#FFFFFF",
    marker: Callable[[str, str], str] = lambda x, _: x,
) -> str:
    """
    Hand-crafted recursive formatter for more control.
    Each line, if belonging to the current "scope", is indented by `indent_level`

    Parameters
    ----------

    root: DataFrame
        The root from which to general SQL.

    sql_splits: Dict[DataFrame, List[str | DataFrame]]
        Mapping of node to the splits generated by `_split_by_generated_sql`.

    indent_level: int

    background: Callable[[DataFrame], str]
        Designated background color getter.

    marker: Callable[[str, str], str]
        Marker that marks the given string with some color.
    """
    assert isinstance(root, (DataFrame, RelationalGroupedDataFrame))

    splits = sql_splits[root]

    indent = " " * SQL_INDENT * indent_level
    string_builder = []

    for sql_excerpt in splits:
        if isinstance(sql_excerpt, str):
            formatted = marker(f"{indent}{sql_excerpt}", background(root))
        else:
            formatted = _recursive_gen_sql_indent(
                sql_excerpt,
                sql_splits=sql_splits,
                indent_level=indent_level + 1,
                background=background,
                marker=marker,
            )
        string_builder.append(formatted)

    return "\n".join(string_builder)


def _sql_highlighter(
    *, visited: Dict[DataFrame, str], graph: DiGraph
) -> Dict[str, str]:
    """
    Highlight each node in the visited graph.
    """

    color = _color_palatte(graph=graph, visited=visited)
    node_color = {node: color[name] for node, name in visited.items()}

    generated_splits = _gen_sql_splits(visited)

    result = {}
    for node, name in visited.items():
        result[name] = (
            '<div class="code">'
            + _recursive_gen_sql_indent(
                node,
                sql_splits=generated_splits,
                background=lambda x: node_color[x],
                marker=_colorize,
            )
            + "</div>"
        )
    return result


def _gen_sql_splits(
    visited: Dict[DataFrame, str]
) -> Dict[DataFrame, List[str | DataFrame]]:
    """
    Splits each node in the `visited` dict into their dependencies.
    Requires "completeness" for the graph,
    each visited node's parent must also be present in the dict.
    """

    generated_splits: Dict[DataFrame, List[str | DataFrame]] = {}
    # Verify that the generate sqls don't have overlaps
    for node in visited:
        generated_splits[node] = _split_by_generated_sql(node)
        assert len(generated_splits[node])
    return generated_splits


def PONDER_SHOW_FORMATTED_SQL(node: DataFrame) -> str:
    """
    An uncolored but formatted sql string generated by the node.
    """

    _, visited = _visit_nodes(root=node)
    return _recursive_gen_sql_indent(node, sql_splits=_gen_sql_splits(visited))


def _color_palatte(
    graph: DiGraph,
    visited: Dict[DataFrame, str],
) -> Dict[str, str]:
    """
    Returns the node name to color code mapping.
    """

    assert isinstance(graph, nx.DiGraph)

    color: Dict[str, str] = {}

    # Node-Type id mapping for color sequence.
    type_id: Dict[Type[DataFrame], int] = {}
    for node, name in visited.items():
        if _get_node_type(node) in type_id:
            continue

        if len(type_id) == 0:
            type_id[_get_node_type(node)] = 1
        else:
            type_id[_get_node_type(node)] = max(type_id.values()) + 1

    for node, name in visited.items():
        id = type_id[_get_node_type(node)]
        color_seq_idx = id % len(COLOR_SEQ)
        color[name] = COLOR_SEQ[color_seq_idx]
    return color


_TEMPLATE = r"""
<!DOCTYPE html>
<html>

<head>
</head>

<style>
    span {
        display: table;
    }

    .code a,
    .code span,
    .code tr,
    .code td {
        white-space: pre;
        font-family: monospace;
    }

    .row {
        display: flex;
    }

    .column {
        flex: 50%;
    }
</style>

<body>
    <div class="row" , id="all">
        <div class="column" , id="svg"></div>
        <div class="column" , id="sql"></div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/d3@6"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-selection@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-dispatch@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-quadtree@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-timer@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-force@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-array@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-color@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-format@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-interpolate@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-time@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-time-format@4"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-scale@4"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-scale-chromatic@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-drag@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-ease@3"></script>
    <script src="https://cdn.jsdelivr.net/npm/d3-transition@3"></script>
    <script>
        (async () => {
            // Get data
            const text = await (await fetch("./query_tree.json")).text();
            const graph = JSON.parse(text);
            console.log(JSON, JSON.parse);

            // Setup
            const color = d3.scaleOrdinal(d3.schemeTableau10);

            const width = window.innerWidth / 2;
            const height = window.innerHeight;


            const nodes = graph.nodes;
            const links = graph.links;
            console.log("text", text);
            console.log("graph", graph);
            console.log("links", links);

            // D3 does weird things to JSON.parse.
            function parentNodes(current, set) {
                if (set === undefined) {
                    set = new Set();
                }

                if (set.has(current)) {
                    return;
                }

                set.add(current);

                // Get parents
                for (var { source: s, target: t } of links) {
                    if (t.id == current) {
                        parentNodes(s.id, set);
                    }
                }

                return set;
            }

            // Simulate
            const simulation = d3
                .forceSimulation(nodes)
                .velocityDecay(0.1)
                .force("charge", d3.forceManyBody().strength(-500))
                .force(
                    "link",
                    d3.forceLink(links).id((d) => d.id)
                )
                .force("x", d3.forceX(width / 2))
                .force("y", d3.forceY(height / 2))
                .force("collision", d3.forceCollide())
                .on("tick", tick);

            // Generate svg
            const svg_box = d3
                .select("#svg")
                .append("svg")
                .attr("width", width)
                .attr("height", height)
                .attr("viewBox", [0, 0, width, height]);
            const svg = svg_box
                .append("g")
                .attr("style", "max-width: 100%; height: auto;");

            svg
                .append("defs")
                .append("marker")
                .attr("id", "arrowhead")
                .attr("viewBox", "0 -5 10 10")
                .attr("refX", 15)
                .attr("refY", -0.5)
                .attr("markerWidth", 3)
                .attr("markerHeight", 3)
                .attr("orient", "auto")
                .append("path")
                .attr("fill", "#999")
                .attr("d", "M0,-5L10,0L0,5");

            // Tooltips
            const tooltip = d3
                .select("#sql")
                .append("div")
                .style("opacity", 1)
                .attr("class", "tooltip")
                // .style("background-color", "light grey")
                .style("border", "solid")
                .style("border-width", "1px")
                .style("border-radius", "5px")
                .style("padding", "10px");

            const link = svg
                .append("g")
                .attr("stroke", "#999")
                .attr("stroke-opacity", 0.6)
                .selectAll("line")
                .data(links)
                .join("line")
                .attr("marker-end", "url(#arrowhead)")
                .attr("stroke-width", 5);

            const node_container = svg
                .append("g")
                .attr("stroke", "#fff")
                .attr("stroke-width", 1.5)
                .selectAll("circle")
                .data(nodes);

            const node = node_container
                .join("circle")
                .attr("r", 15)
                .attr("fill", (d) => d.fillcolor);

            const node_label = node_container
                .join("text")
                .text((d) => d.id)
                .attr("x", 8)
                .attr("y", "0.31em")
                .attr("font-size", "small")
                .clone(true)
                .lower()
                .attr("stroke", "grey")
                .attr("stroke-width", 1);

            node.call(
                d3
                    .drag()
                    .on("start", dragstarted)
                    .on("drag", dragged)
                    .on("end", dragended)
            );

            node
                .on("mouseover", mouseover)
                .on("mousemove", mousemove)
                .on("mouseleave", mouseleave);

            function tick() {
                link
                    .attr("x1", (d) => d.source.x)
                    .attr("y1", (d) => d.source.y)
                    .attr("x2", (d) => d.target.x)
                    .attr("y2", (d) => d.target.y);
                node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);
                node_label.attr("transform", (d) => `translate(${d.x}, ${d.y})`);
            }

            // Reheat the simulation when drag starts, and fix the subject position.
            function dragstarted(event) {
                if (!event.active) simulation.alphaTarget(0.3).restart();
                event.subject.fx = event.subject.x;
                event.subject.fy = event.subject.y;
            }

            // Update the subject (dragged node) position during drag.
            function dragged(event) {
                event.subject.fx = event.x;
                event.subject.fy = event.y;
            }

            // Restore the target alpha so the simulation cools after dragging ends.
            // Unfix the subject position now that it's no longer being dragged.
            function dragended(event) {
                if (!event.active) simulation.alphaTarget(0);
                event.subject.fx = null;
                event.subject.fy = null;
            }

            function mouseover(_, d) {
                const current = d.id;

                // JS defaults to undefined.
                const parents = parentNodes(current);
                console.log(current, parents);

                node.attr("opacity", d => parents.has(d.id) ? 1 : 0.5);
            }

            function mousemove(event, d) {
                const extras = JSON.stringify(d.extras);
                tooltip.html(`${d.sql} <br>
                    <b>Extra Info</b><br>
                    <pre style="white-space: nowrap">${extras}</pre>`);
            }

            function mouseleave(_, d) {
            }

        })();
    </script>
</body>

</html>
"""
"""
Not ideal, but still the most portable way to embed HTML."""
