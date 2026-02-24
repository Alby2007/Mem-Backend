"""
knowledge/kb_repair_proposals.py — KB Repair Proposals (Phase 5)

Given an InsufficiencyDiagnosis, generates structured repair proposals with:
  - Preview: concrete atom IDs, merge clusters, adjacency data (never executed)
  - Simulation: arithmetic signal deltas if repair were applied
  - Validation: specific signal target + recheck window

Design invariants:
  - Never modifies the facts table
  - Never executes any repair
  - Zero-LLM, pure Python, <10ms per call
  - Multi-proposal per diagnosis (top N matched types, capped at 3)
  - Confidence gating for risky strategies (schema/semantic mutation)
  - Adjacency-aware INGEST_MISSING (distinguishes true absence from graph isolation)
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple

try:
    from knowledge.kb_insufficiency_classifier import InsufficiencyType, InsufficiencyDiagnosis
    _HAS_CLASSIFIER = True
except ImportError:
    _HAS_CLASSIFIER = False
    InsufficiencyType = None
    InsufficiencyDiagnosis = None

try:
    from knowledge.authority import get_authority
    _HAS_AUTHORITY = True
except ImportError:
    _HAS_AUTHORITY = False
    def get_authority(source: str) -> float:
        return 0.5

try:
    from knowledge.kb_domain_schemas import (
        DOMAIN_PREDICATE_SCHEMAS,
        detect_topic_domain,
        missing_schema_predicates,
        schema_completeness,
    )
    _HAS_DOMAIN_SCHEMAS = True
except ImportError:
    _HAS_DOMAIN_SCHEMAS = False
    DOMAIN_PREDICATE_SCHEMAS = {}
    def detect_topic_domain(topic, atoms): return None
    def missing_schema_predicates(domain, preds): return []
    def schema_completeness(domain, preds): return 1.0

try:
    from knowledge.kb_validation import validate_all, governance_verdict
    _HAS_VALIDATION = True
except ImportError:
    _HAS_VALIDATION = False


# ── Constants ──────────────────────────────────────────────────────────────────

_MAX_PROPOSALS     = 3
_CONFIDENCE_FLOOR  = 0.30
_MAX_ATOMS_QUERY   = 200
_ADJACENCY_MIN     = 3       # threshold: isolated vs genuinely sparse
_MERGE_JACCARD_THR = 0.60
_LOW_AUTH_CUTOFF   = 0.50
_REWEIGHT_MULT     = 0.70
_DEDUP_JACCARD_THR = 0.50

_RISKY_CONFIDENCE = {
    'MERGE_ATOMS':           0.75,
    'INTRODUCE_PREDICATES':  0.75,
    'SPLIT_DOMAIN':          0.70,
}
_INTRODUCE_MARGIN = 0.20

_RECHECK_TURNS = {
    'INGEST_MISSING': 10, 'RESOLVE_CONFLICTS': 5, 'MERGE_ATOMS': 5,
    'INTRODUCE_PREDICATES': 8, 'REWEIGHT_SOURCES': 5,
    'DEDUPLICATE': 5, 'SPLIT_DOMAIN': 8, 'RESTORE_ATOMS': 3, 'MANUAL_REVIEW': 15,
}


# ── Enums ──────────────────────────────────────────────────────────────────────

class RepairStrategy(str, Enum):
    INGEST_MISSING       = 'ingest_missing'
    RESOLVE_CONFLICTS    = 'resolve_conflicts'
    MERGE_ATOMS          = 'merge_atoms'
    INTRODUCE_PREDICATES = 'introduce_predicates'
    REWEIGHT_SOURCES     = 'reweight_sources'
    DEDUPLICATE          = 'deduplicate'
    SPLIT_DOMAIN         = 'split_domain'
    RESTORE_ATOMS        = 'restore_atoms'
    MANUAL_REVIEW        = 'manual_review'


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class RepairPreview:
    atoms_to_remove: List[int] = field(default_factory=list)
    atoms_to_merge: List[List[int]] = field(default_factory=list)
    atoms_to_add: List[dict] = field(default_factory=list)
    sources_to_reweight: Dict[str, float] = field(default_factory=dict)
    new_predicates: List[str] = field(default_factory=list)
    sub_topics: List[str] = field(default_factory=list)
    candidate_related_topics: List[str] = field(default_factory=list)
    candidate_source_prefixes: List[str] = field(default_factory=list)
    missing_predicate_clusters: List[str] = field(default_factory=list)
    affected_atom_count: int = 0
    summary: str = ''

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


@dataclass
class RepairSimulation:
    estimated_stress_delta: float = 0.0
    estimated_conflict_delta: float = 0.0
    estimated_authority_delta: float = 0.0
    estimated_entropy_delta: float = 0.0
    estimated_atom_count_delta: int = 0
    confidence: float = 0.0
    assumptions: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'estimated_stress_delta': round(self.estimated_stress_delta, 4),
            'estimated_conflict_delta': round(self.estimated_conflict_delta, 4),
            'estimated_authority_delta': round(self.estimated_authority_delta, 4),
            'estimated_entropy_delta': round(self.estimated_entropy_delta, 4),
            'estimated_atom_count_delta': self.estimated_atom_count_delta,
            'confidence': round(self.confidence, 4),
            'assumptions': self.assumptions,
        }


@dataclass
class ValidationMetric:
    target_signal: str = ''
    target_direction: str = 'decrease'
    target_threshold: float = 0.0
    recheck_after_turns: int = 5
    description: str = ''

    def to_dict(self) -> dict:
        return {
            'target_signal': self.target_signal,
            'target_direction': self.target_direction,
            'target_threshold': round(self.target_threshold, 4),
            'recheck_after_turns': self.recheck_after_turns,
            'description': self.description,
        }


@dataclass
class RepairProposal:
    proposal_id: str
    diagnosis_id: str
    topic: str
    insufficiency_type: object
    strategy: RepairStrategy
    is_primary: bool
    description: str
    preview: RepairPreview
    simulation: RepairSimulation
    validation: ValidationMetric
    generated_at: str

    def debug_str(self) -> str:
        tag = 'PRIMARY ' if self.is_primary else ''
        return (
            f"[RepairProposal] {tag}id={self.proposal_id[:8]}... "
            f"strategy={self.strategy.value}\n"
            f"  preview: {self.preview.summary}\n"
            f"  simulation: stress_delta={self.simulation.estimated_stress_delta:+.2f} "
            f"confidence={self.simulation.confidence:.2f}\n"
            f"  validation: {self.validation.description}"
        )

    def to_db_row(self) -> tuple:
        itype = self.insufficiency_type.value if hasattr(self.insufficiency_type, 'value') else str(self.insufficiency_type)
        return (
            self.proposal_id, self.diagnosis_id, self.topic, itype,
            self.strategy.value, 1 if self.is_primary else 0,
            json.dumps(self.preview.to_dict()),
            json.dumps(self.simulation.to_dict()),
            json.dumps(self.validation.to_dict()),
            self.description, 'pending', self.generated_at,
        )


# ── Atom fetcher ───────────────────────────────────────────────────────────────

def _fetch_topic_atoms(topic: str, db_conn: sqlite3.Connection) -> List[dict]:
    try:
        cursor = db_conn.cursor()
        cursor.execute(
            "SELECT id, subject, predicate, object, source, confidence, confidence_effective "
            "FROM facts WHERE subject LIKE ? OR object LIKE ? ORDER BY id ASC LIMIT ?",
            (f'%{topic}%', f'%{topic}%', _MAX_ATOMS_QUERY),
        )
        cols = ['id', 'subject', 'predicate', 'object', 'source', 'confidence', 'confidence_effective']
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception:
        return []


# ── Per-type proposal generators ───────────────────────────────────────────────

def _propose_ingest_missing(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview()
    try:
        cursor = db_conn.cursor()
        prefix = topic.split()[0] if topic else topic
        cursor.execute(
            "SELECT DISTINCT subject FROM facts WHERE subject LIKE ? AND subject != ? LIMIT 20",
            (f'%{prefix}%', topic),
        )
        related = [r[0] for r in cursor.fetchall()]
        preview.candidate_related_topics = related[:10]
        if related:
            ph = ','.join('?' * len(related[:5]))
            cursor.execute(f"SELECT DISTINCT source FROM facts WHERE subject IN ({ph}) LIMIT 30", related[:5])
            preview.candidate_source_prefixes = list({r[0].split('_')[0] for r in cursor.fetchall() if r[0]})[:8]
            topic_preds = {a['predicate'] for a in atoms if a['predicate']}
            cursor.execute(f"SELECT DISTINCT predicate FROM facts WHERE subject IN ({ph})", related[:5])
            adj_preds = {r[0] for r in cursor.fetchall() if r[0]}
            preview.missing_predicate_clusters = list(adj_preds - topic_preds)[:10]
    except Exception:
        pass

    n = len(preview.candidate_related_topics)
    if n > _ADJACENCY_MIN:
        preview.summary = (f"Topic is graph-isolated ({n} adjacent topics exist) — bridge connections before ingesting")
    else:
        srcs = ', '.join(preview.candidate_source_prefixes[:3]) or 'none found'
        preview.summary = (f"Topic is genuinely sparse ({len(atoms)} atoms) — ingest from: {srcs}")
    preview.affected_atom_count = len(atoms)

    sim = RepairSimulation(estimated_stress_delta=-0.20, estimated_entropy_delta=+0.15, confidence=0.55,
        assumptions=['New atoms will come from diverse sources', 'Ingestion will not introduce new conflicts'])
    val = ValidationMetric(target_signal='domain_entropy', target_direction='increase', target_threshold=0.50,
        recheck_after_turns=_RECHECK_TURNS['INGEST_MISSING'],
        description=f"domain_entropy should rise above 0.50 after {_RECHECK_TURNS['INGEST_MISSING']} turns")
    return preview, sim, val


def _propose_resolve_conflicts(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview()
    by_subject: Dict[str, List[dict]] = defaultdict(list)
    for a in atoms:
        if a['subject']:
            by_subject[a['subject']].append(a)
    to_remove = []
    for subj, group in by_subject.items():
        if len(group) < 2:
            continue
        grp = sorted(group, key=lambda x: x['confidence_effective'] or 0.0, reverse=True)
        for i in range(1, len(grp)):
            if ((grp[0]['confidence_effective'] or 0.0) - (grp[i]['confidence_effective'] or 0.0)) > 0.30:
                to_remove.append(grp[i]['id'])
    preview.atoms_to_remove = to_remove
    preview.affected_atom_count = len(to_remove)
    preview.summary = f"{len(to_remove)} atoms flagged for removal (conflict pairs, confidence divergence > 0.30)"

    conflict = signals.get('conflict_cluster', 0.0)
    sim = RepairSimulation(estimated_stress_delta=-0.15, estimated_conflict_delta=-(conflict * 0.60), confidence=0.68,
        assumptions=['Lower-confidence atom in each pair is incorrect', 'Removal resolves conflict'])
    val = ValidationMetric(target_signal='conflict_cluster', target_direction='decrease', target_threshold=0.35,
        recheck_after_turns=_RECHECK_TURNS['RESOLVE_CONFLICTS'],
        description=f"conflict_cluster should drop below 0.35 after {_RECHECK_TURNS['RESOLVE_CONFLICTS']} turns")
    return preview, sim, val


def _propose_merge_atoms(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview()
    by_pred: Dict[str, List[dict]] = defaultdict(list)
    for a in atoms:
        if a['predicate']:
            by_pred[a['predicate']].append(a)
    clusters = []
    for pred, group in by_pred.items():
        if len(group) < 2:
            continue
        tsets = [(a['id'], set((a['object'] or '').lower().split())) for a in group]
        visited = set()
        for i, (id_i, ts_i) in enumerate(tsets):
            if id_i in visited or not ts_i:
                continue
            cluster = [id_i]
            for j, (id_j, ts_j) in enumerate(tsets):
                if i == j or id_j in visited or not ts_j:
                    continue
                u = len(ts_i | ts_j)
                if u > 0 and len(ts_i & ts_j) / u >= _MERGE_JACCARD_THR:
                    cluster.append(id_j)
                    visited.add(id_j)
            if len(cluster) >= 2:
                clusters.append(cluster)
                visited.add(id_i)
    preview.atoms_to_merge = clusters
    affected = sum(len(c) - 1 for c in clusters)
    preview.affected_atom_count = affected
    preview.summary = f"{len(clusters)} merge clusters ({affected} atoms would be consolidated)"

    sim = RepairSimulation(estimated_stress_delta=-0.10, estimated_atom_count_delta=-affected, confidence=0.60,
        assumptions=['Longest object in cluster is most complete', 'Merge preserves all distinct information'])
    val = ValidationMetric(target_signal='predicate_diversity', target_direction='increase', target_threshold=0.20,
        recheck_after_turns=_RECHECK_TURNS['MERGE_ATOMS'],
        description=f"predicate_diversity should rise above 0.20 after {_RECHECK_TURNS['MERGE_ATOMS']} turns")
    return preview, sim, val


def _is_code_token(w: str) -> bool:
    """Return True if the token looks like a code artifact rather than a semantic term."""
    import re
    # Python keywords and builtins
    _CODE_KEYWORDS = {
        'return', 'class', 'def', 'self', 'none', 'true', 'false', 'import',
        'from', 'raise', 'pass', 'yield', 'async', 'await', 'lambda', 'with',
        'assert', 'except', 'finally', 'global', 'nonlocal', 'elif', 'else',
        'print', 'type', 'list', 'dict', 'tuple', 'str', 'int', 'float', 'bool',
        'args', 'kwargs', 'value', 'values', 'result', 'results', 'param',
        'params', 'variable', 'variables', 'object', 'objects', 'instance',
        'method', 'function', 'attribute', 'property', 'module', 'package',
        'error', 'exception', 'index', 'item', 'items', 'element', 'elements',
        'optional', 'returns', 'default', 'example', 'note', 'todo', 'fixme',
    }
    if w in _CODE_KEYWORDS:
        return True
    # dunder patterns: __init__, __str__, etc.
    if re.match(r'^__\w+__$', w):
        return True
    # looks like a function call or decorator
    if w.endswith('(') or w.startswith('@'):
        return True
    # contains punctuation typical of code (parens, brackets, colons, dots)
    if re.search(r'[(){}\[\]<>:;,=+\-*/%|&^~\\]', w):
        return True
    # camelCase internal caps (e.g. getConfig, myVar) — code identifiers
    if re.search(r'[a-z][A-Z]', w):
        return True
    # all-caps acronym-style (e.g. HTTP is fine, but XYZABC is noise)
    if len(w) > 5 and w.isupper():
        return True
    # numeric or starts with digit
    if re.match(r'^\d', w):
        return True
    return False


def _propose_introduce_predicates(topic, signals, atoms, diagnosis, db_conn):
    import re
    preview = RepairPreview()

    existing_preds = {(a['predicate'] or '').lower() for a in atoms if a['predicate']}

    # ── Path A: Schema completion (domain known) ──────────────────────────────
    # Deterministically detect domain. If known, propose the required predicates
    # that are absent — this is ontology-guided schema repair, not vocabulary mining.
    domain = detect_topic_domain(topic, atoms)
    missing = missing_schema_predicates(domain, existing_preds) if domain else []

    if missing:
        candidates = missing[:5]
        preview.new_predicates = candidates
        preview.affected_atom_count = len(atoms)
        preview.summary = (
            f"Schema completion ({domain}): missing predicates: {', '.join(candidates)}"
        )
        sim = RepairSimulation(
            estimated_stress_delta=-0.15,
            estimated_entropy_delta=+0.10,
            confidence=0.70,
            assumptions=[
                f'Topic classified as domain={domain!r}',
                'Missing required predicates identified from domain schema',
                'Adoption via re-ingestion with schema-guided extraction',
            ],
        )
        val = ValidationMetric(
            target_signal='predicate_diversity',
            target_direction='increase',
            target_threshold=0.20,
            recheck_after_turns=_RECHECK_TURNS['INTRODUCE_PREDICATES'],
            description=(
                f"predicate_diversity should rise above 0.20 after "
                f"{_RECHECK_TURNS['INTRODUCE_PREDICATES']} turns "
                f"(domain={domain}, {len(missing)} predicates missing from schema)"
            ),
        )
        return preview, sim, val

    # ── Path B: Frequency-based extraction (domain unknown / schema complete) ──
    # Fallback: extract high-frequency semantic terms from object text.
    # Downweight code-heavy atoms; filter code tokens.
    _STOP = {
        'the','a','an','is','are','was','were','be','been','being','have','has','had',
        'do','does','did','will','would','could','should','may','might','shall','can',
        'of','in','on','at','to','for','with','by','from','and','or','but','not',
        'that','this','it','its','also','when','where','which','who','what','how',
        'then','than','such','each','both','more','most','some','any','all','other',
        'used','uses','using','based','provides','allows','returns','given','called',
        'takes','makes','gets','sets','adds','creates','builds','runs','loads',
    }
    _SEMANTIC_PREDS = {'description', 'readme', 'purpose', 'summary', 'about',
                       'feature', 'capability', 'integrates_with', 'has_feature',
                       'has_purpose', 'target_user', 'ui_framework', 'language'}
    _CODE_PREDS     = {'source_code', 'has_function', 'has_class', 'has_method',
                       'has_module', 'code_snippet'}

    counts: Counter = Counter()
    for a in atoms:
        pred = (a['predicate'] or '').lower()
        weight = 0.2 if pred in _CODE_PREDS else (3.0 if pred in _SEMANTIC_PREDS else 1.0)
        obj = re.sub(r'```[\s\S]*?```', ' ', a['object'] or '')
        obj = re.sub(r'`[^`]+`', ' ', obj)
        for raw in obj.lower().split():
            w = raw.strip('.,;:!?"\'/\\()[]{}')
            if len(w) >= 5 and w not in _STOP and w not in existing_preds and not _is_code_token(w):
                counts[w] += weight

    candidates = [w for w, _ in counts.most_common(20) if len(w) >= 5][:5]
    if not candidates:
        raw_counts: Counter = Counter()
        for a in atoms:
            for w in (a['object'] or '').lower().split():
                w = w.strip('.,;:!?"\'/\\()[]{}')
                if len(w) > 4 and w not in _STOP and w not in existing_preds:
                    raw_counts[w] += 1
        candidates = [w for w, _ in raw_counts.most_common(5)]

    domain_note = f" (domain={domain}, schema complete)" if domain else " (domain unknown)"
    preview.new_predicates = candidates
    preview.affected_atom_count = len(atoms)
    preview.summary = (
        f"Proposed semantic predicates{domain_note}: {', '.join(candidates)}"
        if candidates else "No semantic predicate candidates found"
    )
    sim = RepairSimulation(
        estimated_stress_delta=-0.10,
        estimated_entropy_delta=+0.05,
        confidence=0.40,
        assumptions=['Frequency-based extraction (domain schema not applicable)',
                     'New predicates applied via re-ingestion'],
    )
    val = ValidationMetric(
        target_signal='predicate_diversity',
        target_direction='increase',
        target_threshold=0.15,
        recheck_after_turns=_RECHECK_TURNS['INTRODUCE_PREDICATES'],
        description=(
            f"predicate_diversity should rise above 0.15 after "
            f"{_RECHECK_TURNS['INTRODUCE_PREDICATES']} turns"
        ),
    )
    return preview, sim, val


def _propose_reweight_sources(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview()
    reweight = {}
    for a in atoms:
        src = a['source']
        if not src or src in reweight:
            continue
        auth = get_authority(src)
        if auth < _LOW_AUTH_CUTOFF:
            conf = a['confidence_effective'] or a['confidence'] or 0.5
            reweight[src] = round(conf * _REWEIGHT_MULT, 4)
    preview.sources_to_reweight = reweight
    preview.affected_atom_count = sum(1 for a in atoms if a['source'] in reweight)
    preview.summary = f"{len(reweight)} low-authority sources for confidence rescaling ({preview.affected_atom_count} atoms)"

    auth_conflict = signals.get('authority_conflict', 0.0)
    sim = RepairSimulation(estimated_stress_delta=-0.12, estimated_authority_delta=-(auth_conflict * 0.50), confidence=0.65,
        assumptions=[f'Multiplying confidence_effective by {_REWEIGHT_MULT}', 'Authority scores are accurate'])
    val = ValidationMetric(target_signal='authority_conflict', target_direction='decrease', target_threshold=0.40,
        recheck_after_turns=_RECHECK_TURNS['REWEIGHT_SOURCES'],
        description=f"authority_conflict should drop below 0.40 after {_RECHECK_TURNS['REWEIGHT_SOURCES']} turns")
    return preview, sim, val


def _propose_deduplicate(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview()
    items = [(a['id'], set((a['object'] or '').lower().split()),
              a['confidence_effective'] or a['confidence'] or 0.0) for a in atoms if a['object']]
    sample = sorted(items, key=lambda x: list(x[1]))[:50]
    to_remove = set()
    for i in range(len(sample)):
        for j in range(i + 1, len(sample)):
            id_i, ts_i, c_i = sample[i]
            id_j, ts_j, c_j = sample[j]
            if not ts_i or not ts_j:
                continue
            u = len(ts_i | ts_j)
            if u > 0 and len(ts_i & ts_j) / u >= _DEDUP_JACCARD_THR:
                to_remove.add(id_j if c_i >= c_j else id_i)
    preview.atoms_to_remove = list(to_remove)
    preview.affected_atom_count = len(to_remove)
    preview.summary = f"{len(to_remove)} near-duplicate atoms identified (Jaccard ≥ 0.50, keeping higher-confidence)"

    sim = RepairSimulation(estimated_stress_delta=-0.08, estimated_atom_count_delta=-len(to_remove), confidence=0.70,
        assumptions=['Higher-confidence atom is canonical', 'Jaccard on word tokens captures paraphrase redundancy'])
    val = ValidationMetric(target_signal='object_similarity', target_direction='decrease', target_threshold=0.25,
        recheck_after_turns=_RECHECK_TURNS['DEDUPLICATE'],
        description=f"object_similarity should drop below 0.25 after {_RECHECK_TURNS['DEDUPLICATE']} turns")
    return preview, sim, val


def _propose_split_domain(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview()
    prefix_groups: Dict[str, List[int]] = defaultdict(list)
    for a in atoms:
        prefix_groups[(a['source'] or 'unknown').split('_')[0]].append(a['id'])
    top = sorted(prefix_groups.keys(), key=lambda p: len(prefix_groups[p]), reverse=True)[:5]
    preview.sub_topics = [f'{topic}::{p}' for p in top]
    preview.affected_atom_count = len(atoms)
    preview.summary = f"Proposed {len(preview.sub_topics)} sub-topic partitions by source prefix: {', '.join(top)}"

    entropy = signals.get('domain_entropy', 0.0)
    sim = RepairSimulation(estimated_stress_delta=-0.15, estimated_entropy_delta=-(entropy * 0.40), confidence=0.55,
        assumptions=['Source prefix is reliable domain boundary proxy', 'Sub-topics will have lower entropy'])
    val = ValidationMetric(target_signal='domain_entropy', target_direction='decrease', target_threshold=0.65,
        recheck_after_turns=_RECHECK_TURNS['SPLIT_DOMAIN'],
        description=f"domain_entropy per sub-topic should drop below 0.65 after {_RECHECK_TURNS['SPLIT_DOMAIN']} turns")
    return preview, sim, val


def _propose_restore_atoms(topic, signals, atoms, diagnosis, db_conn):
    """Propose reinstatement of soft-deleted atoms when domain_entropy has collapsed."""
    entropy = signals.get('domain_entropy', 1.0)
    preview = RepairPreview(
        affected_atom_count=0,
        summary=(
            f"Entropy collapse detected (domain_entropy={entropy:.4f}). "
            f"Propose reinstatement of soft-deleted atoms from under-represented sources."
        ),
    )
    preview.to_dict()['entropy_before'] = entropy  # passed through to executor

    sim = RepairSimulation(
        estimated_stress_delta=-0.02,
        estimated_entropy_delta=+0.008,
        estimated_atom_count_delta=+50,
        confidence=0.65,
        assumptions=[
            'Soft-deleted atoms from under-represented sources will increase entropy',
            'Reinstated atoms do not reintroduce conflicts (original confidence preserved)',
        ],
    )
    val = ValidationMetric(
        target_signal='domain_entropy',
        target_direction='increase',
        target_threshold=entropy + 0.005,
        recheck_after_turns=_RECHECK_TURNS['RESTORE_ATOMS'],
        description=(
            f"domain_entropy should rise above {entropy + 0.005:.4f} "
            f"after {_RECHECK_TURNS['RESTORE_ATOMS']} turns"
        ),
    )
    return preview, sim, val


def _propose_manual_review(topic, signals, atoms, diagnosis, db_conn):
    preview = RepairPreview(affected_atom_count=len(atoms),
        summary=f"No automated strategy — {len(atoms)} atoms require manual inspection")
    sim = RepairSimulation(confidence=0.0, assumptions=['Manual intervention required'])
    val = ValidationMetric(target_signal='composite_stress', target_direction='decrease', target_threshold=0.50,
        recheck_after_turns=_RECHECK_TURNS['MANUAL_REVIEW'],
        description="composite_stress should drop below 0.50 after manual intervention")
    return preview, sim, val


# ── Strategy dispatch ──────────────────────────────────────────────────────────

_PROPOSE_FN = {
    RepairStrategy.INGEST_MISSING:       _propose_ingest_missing,
    RepairStrategy.RESOLVE_CONFLICTS:    _propose_resolve_conflicts,
    RepairStrategy.MERGE_ATOMS:          _propose_merge_atoms,
    RepairStrategy.INTRODUCE_PREDICATES: _propose_introduce_predicates,
    RepairStrategy.REWEIGHT_SOURCES:     _propose_reweight_sources,
    RepairStrategy.DEDUPLICATE:          _propose_deduplicate,
    RepairStrategy.SPLIT_DOMAIN:         _propose_split_domain,
    RepairStrategy.RESTORE_ATOMS:        _propose_restore_atoms,
    RepairStrategy.MANUAL_REVIEW:        _propose_manual_review,
}

_TYPE_TO_STRATEGY: Dict = {}


def _ensure_type_map() -> None:
    global _TYPE_TO_STRATEGY
    if _TYPE_TO_STRATEGY or not _HAS_CLASSIFIER:
        return
    _TYPE_TO_STRATEGY = {
        InsufficiencyType.COVERAGE_GAP:                 RepairStrategy.INGEST_MISSING,
        InsufficiencyType.REPRESENTATION_INCONSISTENCY: RepairStrategy.RESOLVE_CONFLICTS,
        InsufficiencyType.GRANULARITY_TOO_FINE:         RepairStrategy.MERGE_ATOMS,
        InsufficiencyType.MISSING_SCHEMA:               RepairStrategy.INTRODUCE_PREDICATES,
        InsufficiencyType.AUTHORITY_IMBALANCE:          RepairStrategy.REWEIGHT_SOURCES,
        InsufficiencyType.SEMANTIC_DUPLICATION:         RepairStrategy.DEDUPLICATE,
        InsufficiencyType.DOMAIN_BOUNDARY_COLLAPSE:     RepairStrategy.SPLIT_DOMAIN,
        InsufficiencyType.SEMANTIC_INCOHERENCE:         RepairStrategy.RESTORE_ATOMS,
        InsufficiencyType.CROSS_TOPIC_DRIFT:            RepairStrategy.MANUAL_REVIEW,
        InsufficiencyType.UNKNOWN:                      RepairStrategy.MANUAL_REVIEW,
    }


# ── Signal margin helper ───────────────────────────────────────────────────────

def _signal_margin(diagnosis) -> float:
    """Confidence weight margin between top and second matched type."""
    if not hasattr(diagnosis, '_matched_weights') or len(diagnosis._matched_weights) < 2:
        return 1.0  # only one type — unambiguous
    weights = sorted(diagnosis._matched_weights, reverse=True)
    return weights[0] - weights[1]


# ── Top-level generator ────────────────────────────────────────────────────────

def generate_repair_proposals(
    diagnosis,              # InsufficiencyDiagnosis
    db_conn: sqlite3.Connection,
    diagnosis_id: str = '',
) -> List[RepairProposal]:
    """
    Generate up to _MAX_PROPOSALS repair proposals for a diagnosis event.

    Returns list sorted by confidence weight descending.
    First proposal has is_primary=True.
    Risky strategies are gated by diagnosis.confidence thresholds.
    """
    _ensure_type_map()

    if not _HAS_CLASSIFIER or diagnosis is None:
        return []

    topic = getattr(diagnosis, 'topic', 'unknown')
    signals = getattr(diagnosis, 'signals', {})
    diag_confidence = getattr(diagnosis, 'confidence', 0.0)
    types = getattr(diagnosis, 'types', [])

    # Fetch atoms once — shared across all proposal generators
    atoms = _fetch_topic_atoms(topic, db_conn)

    # ── Governance Hook 2: confidence penalty ─────────────────────────────
    # Validation severity reduces effective confidence before gating checks.
    # This raises the bar for risky strategies on semantically unstable topics.
    _gov_verdict = None
    if _HAS_VALIDATION:
        try:
            _gov_verdict = governance_verdict(validate_all(topic, db_conn), conn=db_conn)
            diag_confidence = max(0.0, diag_confidence - _gov_verdict.confidence_penalty)
            if _gov_verdict.confidence_penalty > 0:
                print(
                    f"[RepairProposals] governance penalty={_gov_verdict.confidence_penalty:.2f} "
                    f"applied to topic={topic!r}; effective_confidence={diag_confidence:.2f}"
                )
        except Exception as _gov_err:
            print(f"[RepairProposals] governance check error (non-fatal): {_gov_err}")
    now = datetime.now(timezone.utc).isoformat()
    proposals = []

    for itype in types[:_MAX_PROPOSALS]:
        strategy = _TYPE_TO_STRATEGY.get(itype, RepairStrategy.MANUAL_REVIEW)
        strategy_name = strategy.name

        # Confidence gating for risky strategies
        gated_out = False
        gate_reason = ''
        if strategy_name in _RISKY_CONFIDENCE:
            min_conf = _RISKY_CONFIDENCE[strategy_name]
            if diag_confidence < min_conf:
                gated_out = True
                gate_reason = f"diagnosis.confidence={diag_confidence:.2f} < required {min_conf:.2f}"
            elif strategy_name == 'INTRODUCE_PREDICATES':
                margin = _signal_margin(diagnosis)
                if margin < _INTRODUCE_MARGIN:
                    gated_out = True
                    gate_reason = f"signal margin={margin:.2f} < required {_INTRODUCE_MARGIN:.2f}"

        if gated_out:
            # Downgrade to MANUAL_REVIEW with explanation
            actual_strategy = RepairStrategy.MANUAL_REVIEW
            description = (
                f"Strategy {strategy.value} gated out: {gate_reason}. "
                f"Manual review required for {itype.value} insufficiency."
            )
            propose_fn = _propose_manual_review
        else:
            actual_strategy = strategy
            description = f"Repair proposal for {itype.value} insufficiency via {strategy.value}"
            propose_fn = _PROPOSE_FN.get(strategy, _propose_manual_review)

        try:
            preview, sim, val = propose_fn(topic, signals, atoms, diagnosis, db_conn)
        except Exception as e:
            print(f"[RepairProposals] proposal generation error for {itype}: {e}")
            preview, sim, val = _propose_manual_review(topic, signals, atoms, diagnosis, db_conn)
            actual_strategy = RepairStrategy.MANUAL_REVIEW
            description = f"Proposal generation failed for {itype.value}: {e}"

        proposal = RepairProposal(
            proposal_id=uuid.uuid4().hex,
            diagnosis_id=diagnosis_id,
            topic=topic,
            insufficiency_type=itype,
            strategy=actual_strategy,
            is_primary=(len(proposals) == 0),
            description=description,
            preview=preview,
            simulation=sim,
            validation=val,
            generated_at=now,
        )
        proposals.append(proposal)
        print(proposal.debug_str())

    return proposals


# ── Schema ─────────────────────────────────────────────────────────────────────

_CREATE_REPAIR_PROPOSALS = """
CREATE TABLE IF NOT EXISTS repair_proposals (
    id                  TEXT PRIMARY KEY,
    diagnosis_id        TEXT,
    topic               TEXT,
    insufficiency_type  TEXT,
    strategy            TEXT,
    is_primary          INTEGER DEFAULT 0,
    preview_json        TEXT,
    simulation_json     TEXT,
    validation_json     TEXT,
    description         TEXT,
    status              TEXT DEFAULT 'pending',
    generated_at        TEXT NOT NULL,
    resolved_at         TEXT
)
"""


def ensure_repair_proposals_table(conn: sqlite3.Connection) -> None:
    """Idempotent migration. Safe to call on every startup."""
    conn.execute(_CREATE_REPAIR_PROPOSALS)
    conn.commit()


def persist_proposals(proposals: List[RepairProposal], conn: sqlite3.Connection) -> None:
    """Write all proposals to repair_proposals table."""
    if not proposals:
        return
    ensure_repair_proposals_table(conn)
    conn.executemany(
        "INSERT OR IGNORE INTO repair_proposals "
        "(id, diagnosis_id, topic, insufficiency_type, strategy, is_primary, "
        "preview_json, simulation_json, validation_json, description, status, generated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [p.to_db_row() for p in proposals],
    )
    conn.commit()
