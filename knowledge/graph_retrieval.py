"""
Trading KB Graph Retrieval — atom-to-graph pipeline

Converts flat retrieved atoms into a traversable knowledge graph structure.
Runs centrality analysis, path-finding between query concepts, and concept
clustering to produce a graph-structured context for synthesis.

Useful for surfacing cross-instrument relationships, regime–signal chains,
and thesis–evidence connectivity.

Importance formula (Krish's weighted score):
    importance[i] = α·pagerank[i] + β·degree[i] + γ·recency[i]
                  + δ·frequency[i] + ε·confidence[i]

    α=0.30  PageRank structural centrality
    β=0.20  Degree centrality (hub-ness)
    γ=0.25  Recency — confidence_effective (decay-adjusted) if available
    δ=0.10  Frequency — hit_count / max_hit_count (stub until hit tracking PR)
    ε=0.15  Base confidence

δ is a stub: hit_count column exists in schema but stays 0 until the
hit-tracking PR lands. When it does, no formula change is needed.
"""

import re
from collections import defaultdict, deque
from typing import List, Dict, Tuple, Optional, Set

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

# ── Importance weights ─────────────────────────────────────────────────────────
_W_PAGERANK   = 0.30
_W_DEGREE     = 0.20
_W_RECENCY    = 0.25
_W_FREQUENCY  = 0.10  # stub — requires hit_count column (see hit-tracking PR)
_W_CONFIDENCE = 0.15


# ============================================================
# Per-node epistemic score helpers
# ============================================================

def _build_node_scores(
    atoms: List[Dict],
    node_to_idx: Dict[str, int],
) -> Tuple[Dict[int, float], Dict[int, float], Dict[int, float]]:
    """
    Derive per-node recency, frequency, and confidence scores from atoms.

    recency[i]    — max confidence_effective across atoms whose subject/object
                    maps to node i. Falls back to base confidence if
                    confidence_effective is absent (decay worker not yet run).
    frequency[i]  — normalised hit_count. Stub: returns 0.0 until the
                    hit-tracking PR adds hit_count increments on retrieval.
    confidence[i] — max base confidence across atoms mapped to node i.

    All three dicts are normalised to [0, 1] relative to their max value.
    """
    recency_raw:    Dict[int, float] = defaultdict(float)
    frequency_raw:  Dict[int, float] = defaultdict(float)
    confidence_raw: Dict[int, float] = defaultdict(float)

    for atom in atoms:
        subj = atom.get('subject', '').strip()
        obj  = atom.get('object',  '').strip()
        if len(obj) > 80:
            obj = obj[:80].rsplit(' ', 1)[0]

        base_conf = float(atom.get('confidence', 0.5) or 0.5)
        eff_conf  = float(atom.get('confidence_effective') or base_conf)
        hit_count = float(atom.get('hit_count', 0) or 0)

        for node in (subj, obj):
            if node not in node_to_idx:
                continue
            idx = node_to_idx[node]
            recency_raw[idx]    = max(recency_raw[idx],    eff_conf)
            frequency_raw[idx]  = max(frequency_raw[idx],  hit_count)
            confidence_raw[idx] = max(confidence_raw[idx], base_conf)

    def _normalise(d: Dict[int, float]) -> Dict[int, float]:
        mx = max(d.values()) if d else 1.0
        if mx == 0:
            return {k: 0.0 for k in d}
        return {k: v / mx for k, v in d.items()}

    return (
        _normalise(recency_raw),
        _normalise(frequency_raw),
        _normalise(confidence_raw),
    )


# ============================================================
# Atom → Graph conversion (from LLTM)
# ============================================================

def atoms_to_graph(atoms: List[Dict]) -> Tuple[List[str], List[Tuple], Dict]:
    """
    Convert retrieved knowledge atoms to an indexed graph.
    nodes = unique subjects + objects (concepts)
    edges = predicate relationships weighted by confidence

    Returns: (nodes, edges, node_to_idx)
    edges format: (src_idx, tgt_idx, weight, predicate)
    """
    nodes_set = set()
    raw_edges = []

    for atom in atoms:
        subj = atom.get('subject', '').strip()
        obj = atom.get('object', '').strip()
        pred = atom.get('predicate', '').strip()
        conf = float(atom.get('confidence', 0.5) or 0.5)

        if not subj or not obj:
            continue

        # Truncate long object strings to concept level
        if len(obj) > 80:
            obj = obj[:80].rsplit(' ', 1)[0]

        # Skip pure source code objects (too large, not concept-level)
        if pred == 'source_code':
            # Keep just the subject as a node, skip the code body as object
            nodes_set.add(subj)
            continue

        nodes_set.add(subj)
        nodes_set.add(obj)
        raw_edges.append((subj, obj, conf, pred))

    nodes = sorted(nodes_set)
    node_to_idx = {n: i for i, n in enumerate(nodes)}

    # Deduplicate edges, keeping max weight
    edge_dict = {}
    for src, tgt, w, pred in raw_edges:
        if src not in node_to_idx or tgt not in node_to_idx:
            continue
        si, ti = node_to_idx[src], node_to_idx[tgt]
        key = (si, ti)
        if key not in edge_dict or w > edge_dict[key][0]:
            edge_dict[key] = (w, pred)

    edges = [(si, ti, w, pred) for (si, ti), (w, pred) in edge_dict.items()]

    return nodes, edges, node_to_idx


# ============================================================
# Graph Analysis (from LLTM:analyze_graph)
# ============================================================

def compute_degree_centrality(nodes: List[str], edges: List[Tuple]) -> Dict[int, float]:
    """Compute degree centrality for each node."""
    n = len(nodes)
    if n <= 1:
        return {i: 0.0 for i in range(n)}

    degree = defaultdict(float)
    for si, ti, w, _ in edges:
        degree[si] += w
        degree[ti] += w

    max_deg = max(degree.values()) if degree else 1.0
    return {i: degree[i] / max_deg for i in range(n)}


def compute_pagerank(nodes: List[str], edges: List[Tuple],
                     damping: float = 0.85, iterations: int = 30) -> Dict[int, float]:
    """Simple PageRank over the knowledge graph."""
    n = len(nodes)
    if n == 0:
        return {}

    # Build adjacency
    out_edges = defaultdict(list)
    out_weight = defaultdict(float)
    for si, ti, w, _ in edges:
        out_edges[si].append((ti, w))
        out_edges[ti].append((si, w))  # undirected
        out_weight[si] += w
        out_weight[ti] += w

    rank = {i: 1.0 / n for i in range(n)}

    for _ in range(iterations):
        new_rank = {}
        for i in range(n):
            incoming = sum(
                rank[j] * w / max(out_weight[j], 1e-9)
                for j, w in out_edges[i]
            )
            new_rank[i] = (1 - damping) / n + damping * incoming
        rank = new_rank

    # Normalize
    total = sum(rank.values()) or 1.0
    return {i: v / total for i, v in rank.items()}


def bfs_path(src_idx: int, tgt_idx: int, edges: List[Tuple],
             n_nodes: int, max_depth: int = 4) -> Optional[List[int]]:
    """BFS shortest path between two nodes."""
    adj = defaultdict(list)
    for si, ti, w, _ in edges:
        adj[si].append(ti)
        adj[ti].append(si)

    visited = {src_idx}
    queue = deque([(src_idx, [src_idx])])

    while queue:
        node, path = queue.popleft()
        if len(path) > max_depth:
            break
        for neighbor in adj[node]:
            if neighbor == tgt_idx:
                return path + [neighbor]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return None


def find_concept_clusters(nodes: List[str], edges: List[Tuple],
                          min_cluster_size: int = 3) -> List[List[int]]:
    """Find connected components / clusters in the graph."""
    n = len(nodes)
    adj = defaultdict(set)
    for si, ti, w, _ in edges:
        if w > 0.3:  # only strong edges for clustering
            adj[si].add(ti)
            adj[ti].add(si)

    visited = set()
    clusters = []

    for start in range(n):
        if start in visited:
            continue
        cluster = []
        stack = [start]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            cluster.append(node)
            stack.extend(adj[node] - visited)
        if len(cluster) >= min_cluster_size:
            clusters.append(cluster)

    return sorted(clusters, key=len, reverse=True)


# ============================================================
# Query concept extraction
# ============================================================

def extract_query_concepts(user_message: str, nodes: List[str],
                           node_to_idx: Dict) -> List[int]:
    """
    Find which graph nodes correspond to concepts mentioned in the query.
    Returns list of node indices.
    """
    msg_lower = user_message.lower()
    matched = []

    for node, idx in node_to_idx.items():
        node_lower = node.lower()
        # Extract the concept part (after colon if present)
        concept = node_lower.split(':')[-1] if ':' in node_lower else node_lower
        concept_clean = re.sub(r'[_\-]', ' ', concept)

        if (concept_clean in msg_lower or
                (len(concept_clean) > 4 and concept_clean[:6] in msg_lower)):
            matched.append(idx)

    return matched[:10]  # cap at 10 seed concepts


# ============================================================
# Main: build graph-structured context
# ============================================================

def build_graph_context(atoms: List[Dict], user_message: str,
                        max_nodes_in_context: int = 150) -> str:
    """
    Convert atoms to a graph, analyze structure, and return a
    graph-structured context string for Groq synthesis.

    This replaces the flat fact dump with:
    1. Most central concepts (PageRank)
    2. Concept clusters (connected components)
    3. Paths between query concepts
    4. Hub nodes (high-degree connectors)
    """
    if not atoms:
        return ""

    nodes, edges, node_to_idx = atoms_to_graph(atoms)

    if len(nodes) < 5:
        # Too small for graph analysis, return flat
        lines = []
        for a in atoms[:50]:
            lines.append(f"{a.get('subject','')} | {a.get('predicate','')} | {a.get('object','')[:80]}")
        return '\n'.join(lines)

    # Compute centrality
    pagerank = compute_pagerank(nodes, edges)
    degree = compute_degree_centrality(nodes, edges)

    # Per-node epistemic scores (recency, frequency stub, base confidence)
    recency, frequency, confidence_score = _build_node_scores(atoms, node_to_idx)

    # Combined importance score (α/β/γ/δ/ε weighted formula)
    importance = {
        i: (
            _W_PAGERANK   * pagerank.get(i, 0)
            + _W_DEGREE     * degree.get(i, 0)
            + _W_RECENCY    * recency.get(i, 0)
            + _W_FREQUENCY  * frequency.get(i, 0)
            + _W_CONFIDENCE * confidence_score.get(i, 0)
        )
        for i in range(len(nodes))
    }

    # Top central nodes
    top_nodes = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    top_node_indices = {idx for idx, _ in top_nodes[:max_nodes_in_context]}

    # Find query-relevant seed nodes
    seed_nodes = extract_query_concepts(user_message, nodes, node_to_idx)

    # Find clusters
    clusters = find_concept_clusters(nodes, edges)

    # Build context string
    lines = [
        f"KNOWLEDGE GRAPH STRUCTURE: {len(nodes)} concepts, {len(edges)} relationships",
        ""
    ]

    # Section 1: Most central concepts
    lines.append("=== CENTRAL CONCEPTS (highest PageRank) ===")
    for idx, score in top_nodes[:20]:
        node_name = nodes[idx]
        # Find its most important relationships
        node_edges = [(ti if si == idx else si, w, pred)
                      for si, ti, w, pred in edges
                      if si == idx or ti == idx]
        node_edges.sort(key=lambda x: x[1], reverse=True)
        top_rels = [f"{nodes[ni]}({pred})" for ni, w, pred in node_edges[:3]]
        rel_str = " → " + ", ".join(top_rels) if top_rels else ""
        lines.append(f"  [{score:.3f}] {node_name}{rel_str}")

    # Section 2: Concept clusters
    if clusters:
        lines.append("\n=== CONCEPT CLUSTERS ===")
        for i, cluster in enumerate(clusters[:6]):
            cluster_nodes = [nodes[idx] for idx in cluster[:8]]
            lines.append(f"  Cluster {i+1} ({len(cluster)} nodes): {', '.join(cluster_nodes)}")
            if len(cluster) > 8:
                lines.append(f"    ... and {len(cluster)-8} more")

    # Section 3: Paths between query concepts
    if len(seed_nodes) >= 2:
        lines.append("\n=== CONCEPT PATHS (query-relevant connections) ===")
        shown = 0
        for i in range(len(seed_nodes)):
            for j in range(i+1, len(seed_nodes)):
                if shown >= 5:
                    break
                path = bfs_path(seed_nodes[i], seed_nodes[j], edges, len(nodes))
                if path and 2 <= len(path) <= 5:
                    path_str = " → ".join(nodes[idx] for idx in path)
                    lines.append(f"  {path_str}")
                    shown += 1

    # Section 4: All edges involving top nodes (the actual knowledge)
    lines.append("\n=== KEY RELATIONSHIPS ===")
    shown_edges = set()
    edge_lines = []

    # First: edges between top nodes
    for si, ti, w, pred in sorted(edges, key=lambda x: x[2], reverse=True):
        if si in top_node_indices and ti in top_node_indices:
            key = (min(si,ti), max(si,ti))
            if key not in shown_edges:
                shown_edges.add(key)
                edge_lines.append(f"  {nodes[si]} --[{pred}]--> {nodes[ti]}")

    # Then: edges from seed nodes
    for si, ti, w, pred in sorted(edges, key=lambda x: x[2], reverse=True):
        if si in seed_nodes or ti in seed_nodes:
            key = (min(si,ti), max(si,ti))
            if key not in shown_edges:
                shown_edges.add(key)
                edge_lines.append(f"  {nodes[si]} --[{pred}]--> {nodes[ti]}")

    lines.extend(edge_lines[:200])

    if len(edge_lines) > 200:
        lines.append(f"  ... and {len(edge_lines)-200} more relationships")

    return '\n'.join(lines)


def what_do_i_know_about(topic: str, atoms: List[Dict]) -> str:
    """
    LLTM-inspired handle_what_do_i_know: given a topic, traverse the
    knowledge graph to find everything connected to it.

    Returns a structured summary of what JARVIS knows about a topic.
    """
    nodes, edges, node_to_idx = atoms_to_graph(atoms)
    if not nodes:
        return f"No knowledge found about '{topic}'"

    # Find seed nodes matching the topic
    topic_lower = topic.lower()
    seeds = []
    for node, idx in node_to_idx.items():
        concept = node.lower().split(':')[-1] if ':' in node else node.lower()
        if topic_lower in concept or concept in topic_lower:
            seeds.append(idx)

    if not seeds:
        return f"No direct knowledge atoms found for '{topic}'"

    # BFS expansion from seeds (up to depth 3)
    adj = defaultdict(list)
    for si, ti, w, pred in edges:
        adj[si].append((ti, w, pred))
        adj[ti].append((si, w, pred))

    visited = set(seeds)
    frontier = list(seeds)
    all_relevant = list(seeds)
    depth = 0

    while frontier and depth < 3:
        next_frontier = []
        for node in frontier:
            for neighbor, w, pred in adj[node]:
                if neighbor not in visited and w > 0.2:
                    visited.add(neighbor)
                    next_frontier.append(neighbor)
                    all_relevant.append(neighbor)
        frontier = next_frontier
        depth += 1

    # Build summary
    lines = [f"WHAT THE TRADING KB KNOWS ABOUT '{topic.upper()}':"]
    lines.append(f"  {len(all_relevant)} connected concepts found\n")

    # Direct facts
    lines.append("Direct facts:")
    for seed in seeds[:5]:
        seed_edges = [(ti if si == seed else si, pred, w)
                      for si, ti, w, pred in edges
                      if si == seed or ti == seed]
        seed_edges.sort(key=lambda x: x[2], reverse=True)
        for ni, pred, w in seed_edges[:5]:
            lines.append(f"  {nodes[seed]} --[{pred}]--> {nodes[ni]}")

    # Connected concepts
    if len(all_relevant) > len(seeds):
        lines.append("\nConnected concepts:")
        for idx in all_relevant[len(seeds):len(seeds)+20]:
            lines.append(f"  • {nodes[idx]}")

    return '\n'.join(lines)
