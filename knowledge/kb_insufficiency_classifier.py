"""
knowledge/kb_insufficiency_classifier.py — KB Insufficiency Classifier (Phase 4)

When kb_insufficient=True fires, this module diagnoses *which* structural failure
mode is present. Classification precedes repair.

Seven insufficiency types, derived entirely from signals already available in the
facts table + Phase 2 EpistemicStressReport. No LLM calls, no new data collection.

Semantic duplication detection uses pairwise Jaccard similarity on a capped sample
(≤50 atoms). At n=50 this is 1225 comparisons of short strings — negligible cost.
Early-exit guards skip the computation when the signal is clearly absent.

Design invariants:
  - Zero-LLM, pure Python, <5ms per call
  - All signals derived from existing DB tables
  - Returns ranked list (multiple types can co-occur)
  - Never repairs — only classifies
"""

from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

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
    from knowledge.kb_validation import validate_semantics, validate_cross_topic
    _HAS_VALIDATION = True
except ImportError:
    _HAS_VALIDATION = False


# ── Insufficiency types ────────────────────────────────────────────────────────

class InsufficiencyType(str, Enum):
    COVERAGE_GAP                 = 'coverage_gap'
    REPRESENTATION_INCONSISTENCY = 'representation_inconsistency'
    GRANULARITY_TOO_FINE         = 'granularity_too_fine'
    MISSING_SCHEMA               = 'missing_schema'
    AUTHORITY_IMBALANCE          = 'authority_imbalance'
    SEMANTIC_DUPLICATION         = 'semantic_duplication'
    DOMAIN_BOUNDARY_COLLAPSE     = 'domain_boundary_collapse'
    SEMANTIC_INCOHERENCE         = 'semantic_incoherence'
    CROSS_TOPIC_DRIFT            = 'cross_topic_drift'
    UNKNOWN                      = 'unknown'


# ── Thresholds ─────────────────────────────────────────────────────────────────

# COVERAGE_GAP
_COVERAGE_MIN_ATOMS   = 10     # fewer than this → sparse
_COVERAGE_MAX_ENTROPY = 0.40   # AND narrow sourcing

# REPRESENTATION_INCONSISTENCY
_INCONSISTENCY_CONFLICT    = 0.50
_INCONSISTENCY_SUPERSESSION = 0.30

# AUTHORITY_IMBALANCE
_AUTH_CONFLICT_THRESHOLD  = 0.55
_LOW_AUTH_CUTOFF          = 0.50   # atoms below this are "low authority"
_LOW_AUTH_FRACTION        = 0.60   # fraction of low-auth atoms that triggers

# SEMANTIC_DUPLICATION
_DUPLICATION_MIN_ATOMS    = 30
_DUPLICATION_JACCARD_THR  = 0.50   # pair similarity above this = near-duplicate
_DUPLICATION_PAIR_FRAC    = 0.20   # fraction of pairs exceeding threshold
_DUPLICATION_SAMPLE_SIZE  = 50
_DUPLICATION_SKIP_ENTROPY = 0.75   # skip if entropy already high (diverse sourcing)
_DUPLICATION_MIN_SAMPLE   = 10     # skip if sample too small

# GRANULARITY_TOO_FINE
_GRANULARITY_MIN_ATOMS    = 50
_GRANULARITY_MAX_PRED_DIV = 0.15   # predicate_diversity = unique_preds / atom_count
_GRANULARITY_MAX_OBJ_LEN  = 20     # avg object string length

# MISSING_SCHEMA
_SCHEMA_MIN_ATOMS         = 30
_SCHEMA_MAX_PRED_DIV      = 0.10   # even lower predicate diversity

# DOMAIN_BOUNDARY_COLLAPSE
_BOUNDARY_MIN_ENTROPY     = 0.80
_BOUNDARY_MIN_PREFIXES    = 5      # distinct source prefixes

# SEMANTIC_INCOHERENCE (Rule 8 — from kb_validation Layer 2)
_INCOHERENCE_SEVERITY_THRESHOLD = 0.5

# CROSS_TOPIC_DRIFT (Rule 9 — from kb_validation Layer 3)
_CROSS_TOPIC_SEVERITY_THRESHOLD = 0.4


# ── Diagnosis dataclass ────────────────────────────────────────────────────────

@dataclass
class InsufficiencyDiagnosis:
    """
    Classification result for a KB insufficiency event.

    types: ranked list, most confident first. Multiple types can co-occur.
    signals: raw signal values used for classification (for audit/logging).
    confidence: fraction of applicable rules that matched (0-1).
    """
    topic: str
    types: List[InsufficiencyType]
    signals: Dict[str, float]
    confidence: float
    matched_rules: int = 0
    total_rules: int = 9

    def primary_type(self) -> InsufficiencyType:
        return self.types[0] if self.types else InsufficiencyType.UNKNOWN

    def debug_str(self) -> str:
        type_labels = ', '.join(t.value for t in self.types) if self.types else 'unknown'
        sig_str = ' '.join(f"{k}={v:.2f}" for k, v in self.signals.items())
        return (
            f"KBClassifier[topic='{self.topic}' "
            f"types=[{type_labels}] "
            f"confidence={self.confidence:.2f} "
            f"signals: {sig_str}]"
        )

    def to_json(self) -> str:
        return json.dumps({
            'topic': self.topic,
            'types': [t.value for t in self.types],
            'confidence': round(self.confidence, 4),
            'signals': {k: round(v, 4) for k, v in self.signals.items()},
        })


# ── Signal extraction ──────────────────────────────────────────────────────────

def _extract_topic_signals(
    topic: str,
    db_conn: sqlite3.Connection,
    max_atoms: int = 200,
) -> tuple:  # (Dict[str, float], list)
    """
    Extract all classification signals for a topic from the facts table.
    Capped at max_atoms to bound cost.
    Returns (signals_dict, raw_rows) so the caller can reuse rows for domain detection.
    """
    signals: Dict[str, float] = {
        'atom_count':        0.0,
        'predicate_diversity': 0.0,
        'avg_object_length': 0.0,
        'low_auth_fraction': 0.0,
        'object_similarity': 0.0,
        'source_prefix_count': 0.0,
        'authority_std':     0.0,
    }
    raw_rows = []

    try:
        cursor = db_conn.cursor()

        # Fetch atoms for this topic (subject or object contains topic key terms)
        # Use simple LIKE on subject — topic label is usually the subject
        topic_pattern = f'%{topic}%'
        cursor.execute(
            """
            SELECT subject, predicate, object, source, confidence, confidence_effective
            FROM facts
            WHERE subject LIKE ? OR object LIKE ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (topic_pattern, topic_pattern, max_atoms),
        )
        raw_rows = cursor.fetchall()

        if not raw_rows:
            return signals, raw_rows

        rows = raw_rows
        atom_count = len(rows)
        signals['atom_count'] = float(atom_count)

        predicates  = [r[1] for r in rows if r[1]]
        objects     = [r[2] for r in rows if r[2]]
        sources     = [r[3] for r in rows if r[3]]

        # Predicate diversity: unique predicates / atom count
        if predicates:
            signals['predicate_diversity'] = len(set(predicates)) / atom_count

        # Average object string length
        if objects:
            signals['avg_object_length'] = sum(len(o) for o in objects) / len(objects)

        # Authority distribution
        if sources and _HAS_AUTHORITY:
            auth_vals = [get_authority(s) for s in sources]
            signals['low_auth_fraction'] = sum(
                1 for a in auth_vals if a < _LOW_AUTH_CUTOFF
            ) / len(auth_vals)
            # Std dev of authority weights (authority_conflict proxy from raw atoms)
            mean_auth = sum(auth_vals) / len(auth_vals)
            variance = sum((a - mean_auth) ** 2 for a in auth_vals) / len(auth_vals)
            signals['authority_std'] = variance ** 0.5

        # Source prefix count (distinct first segments of source names)
        if sources:
            prefixes = set(s.split('_')[0] for s in sources if s)
            signals['source_prefix_count'] = float(len(prefixes))

        # Pairwise Jaccard similarity (semantic duplication signal)
        signals['object_similarity'] = _jaccard_similarity_sample(
            objects,
            sample_size=_DUPLICATION_SAMPLE_SIZE,
        )

    except Exception as e:
        print(f"[KBClassifier] signal extraction error for topic='{topic}': {e}")

    return signals, raw_rows


def _jaccard_similarity_sample(
    objects: List[str],
    sample_size: int = 50,
) -> float:
    """
    Compute mean pairwise Jaccard similarity on a capped sample of object strings.

    Measures representation redundancy — near-duplicate paraphrases, not just
    exact string duplicates. Lexical, not semantic, but sufficient for structural
    classification.

    Early-exit guards:
      - sample < _DUPLICATION_MIN_SAMPLE → return 0.0 (too few to be meaningful)
      - All objects identical → return 1.0 fast path

    Returns: fraction of pairs exceeding _DUPLICATION_JACCARD_THR
             (not mean similarity — more interpretable as "how many near-dupes")
    """
    if len(objects) < _DUPLICATION_MIN_SAMPLE:
        return 0.0

    # Sample deterministically (sorted then sliced) to avoid random variance
    # across calls for the same topic
    sample = sorted(objects)[:sample_size]

    # Fast path: all identical
    if len(set(sample)) == 1:
        return 1.0

    # Tokenize once
    token_sets = [set(o.lower().split()) for o in sample]

    # Remove empty sets
    token_sets = [ts for ts in token_sets if ts]
    if len(token_sets) < 2:
        return 0.0

    # Pairwise Jaccard — O(n²) on sample, bounded at n=50 → 1225 comparisons max
    n = len(token_sets)
    pairs_above_threshold = 0
    total_pairs = 0

    for i in range(n):
        for j in range(i + 1, n):
            a, b = token_sets[i], token_sets[j]
            intersection = len(a & b)
            union = len(a | b)
            if union == 0:
                continue
            similarity = intersection / union
            total_pairs += 1
            if similarity >= _DUPLICATION_JACCARD_THR:
                pairs_above_threshold += 1

    if total_pairs == 0:
        return 0.0

    return pairs_above_threshold / total_pairs


# ── Classifier ─────────────────────────────────────────────────────────────────

def classify_insufficiency(
    topic: str,
    stress_report,          # EpistemicStressReport — duck-typed to avoid circular import
    db_conn: sqlite3.Connection,
) -> InsufficiencyDiagnosis:
    """
    Classify the type(s) of KB insufficiency for a topic.

    Evaluates all 7 rules in specificity order. Multiple types can match.
    Returns InsufficiencyDiagnosis with ranked list (most confident first).

    All signals derived from existing DB tables + stress_report.
    Zero-LLM, <5ms per call.
    """
    # Extract per-topic signals from DB
    db_signals, raw_rows = _extract_topic_signals(topic, db_conn)

    # Merge with stress_report signals (already computed upstream)
    signals = {
        **db_signals,
        'conflict_cluster':    getattr(stress_report, 'conflict_cluster', 0.0),
        'supersession_density': getattr(stress_report, 'supersession_density', 0.0),
        'authority_conflict':  getattr(stress_report, 'authority_conflict', 0.0),
        'domain_entropy':      getattr(stress_report, 'domain_entropy', 1.0),
    }

    atom_count        = signals['atom_count']
    pred_diversity    = signals['predicate_diversity']
    avg_obj_len       = signals['avg_object_length']
    low_auth_frac     = signals['low_auth_fraction']
    obj_similarity    = signals['object_similarity']
    prefix_count      = signals['source_prefix_count']
    conflict_cluster  = signals['conflict_cluster']
    supersession      = signals['supersession_density']
    auth_conflict     = signals['authority_conflict']
    domain_entropy    = signals['domain_entropy']

    matched: List[Tuple[InsufficiencyType, float]] = []  # (type, confidence_weight)

    # ── Rule 1: COVERAGE_GAP ──────────────────────────────────────────────────
    # Too few atoms AND narrow sourcing → missing data
    if atom_count < _COVERAGE_MIN_ATOMS and domain_entropy < _COVERAGE_MAX_ENTROPY:
        weight = (1.0 - atom_count / _COVERAGE_MIN_ATOMS) * 0.5 + \
                 (1.0 - domain_entropy / _COVERAGE_MAX_ENTROPY) * 0.5
        matched.append((InsufficiencyType.COVERAGE_GAP, weight))

    # ── Rule 2: REPRESENTATION_INCONSISTENCY ──────────────────────────────────
    # High conflict + high supersession → actively contradicted knowledge
    if conflict_cluster > _INCONSISTENCY_CONFLICT and supersession > _INCONSISTENCY_SUPERSESSION:
        weight = (conflict_cluster - _INCONSISTENCY_CONFLICT) / (1.0 - _INCONSISTENCY_CONFLICT) * 0.6 + \
                 (supersession - _INCONSISTENCY_SUPERSESSION) / (1.0 - _INCONSISTENCY_SUPERSESSION) * 0.4
        matched.append((InsufficiencyType.REPRESENTATION_INCONSISTENCY, weight))

    # ── Rule 3: AUTHORITY_IMBALANCE ───────────────────────────────────────────
    # High authority conflict AND dominated by low-authority sources
    if auth_conflict > _AUTH_CONFLICT_THRESHOLD and low_auth_frac > _LOW_AUTH_FRACTION:
        weight = (auth_conflict - _AUTH_CONFLICT_THRESHOLD) / (1.0 - _AUTH_CONFLICT_THRESHOLD) * 0.5 + \
                 (low_auth_frac - _LOW_AUTH_FRACTION) / (1.0 - _LOW_AUTH_FRACTION) * 0.5
        matched.append((InsufficiencyType.AUTHORITY_IMBALANCE, weight))

    # ── Rule 4: SEMANTIC_DUPLICATION ──────────────────────────────────────────
    # Many atoms + high pairwise Jaccard similarity → representation redundancy
    # Early-exit: skip if entropy already high (diverse sourcing → unlikely duplication)
    # or sample too small to be meaningful (already handled in _jaccard_similarity_sample)
    if (atom_count >= _DUPLICATION_MIN_ATOMS
            and domain_entropy < _DUPLICATION_SKIP_ENTROPY
            and obj_similarity > _DUPLICATION_PAIR_FRAC):
        weight = min(1.0, obj_similarity / _DUPLICATION_PAIR_FRAC - 1.0) * 0.7 + \
                 min(1.0, atom_count / (_DUPLICATION_MIN_ATOMS * 2)) * 0.3
        matched.append((InsufficiencyType.SEMANTIC_DUPLICATION, weight))

    # ── Rule 5: GRANULARITY_TOO_FINE ─────────────────────────────────────────
    # Many atoms + low predicate diversity + short objects → over-atomized
    if (atom_count >= _GRANULARITY_MIN_ATOMS
            and pred_diversity < _GRANULARITY_MAX_PRED_DIV
            and avg_obj_len < _GRANULARITY_MAX_OBJ_LEN):
        weight = (1.0 - pred_diversity / _GRANULARITY_MAX_PRED_DIV) * 0.5 + \
                 (1.0 - avg_obj_len / _GRANULARITY_MAX_OBJ_LEN) * 0.3 + \
                 min(1.0, atom_count / (_GRANULARITY_MIN_ATOMS * 2)) * 0.2
        matched.append((InsufficiencyType.GRANULARITY_TOO_FINE, weight))

    # ── Rule 6: MISSING_SCHEMA ────────────────────────────────────────────────
    # Two detection paths:
    #   A) Quantitative: predicate_diversity below threshold (domain-agnostic)
    #   B) Qualitative:  domain known + required predicates absent from atom set
    #      (ontology-guided — fires even if diversity is numerically acceptable)
    schema_matched = False
    schema_weight = 0.0

    if atom_count >= _SCHEMA_MIN_ATOMS and pred_diversity < _SCHEMA_MAX_PRED_DIV:
        schema_weight = (1.0 - pred_diversity / _SCHEMA_MAX_PRED_DIV) * 0.7 + \
                        min(1.0, atom_count / (_SCHEMA_MIN_ATOMS * 3)) * 0.3
        schema_matched = True

    if _HAS_DOMAIN_SCHEMAS and atom_count >= _SCHEMA_MIN_ATOMS:
        rows_for_domain = [{'predicate': r[1], 'object': r[2]} for r in raw_rows]
        topic_domain = detect_topic_domain(topic, rows_for_domain)
        if topic_domain:
            existing_preds = {(r[1] or '').lower() for r in raw_rows if r[1]}
            missing = missing_schema_predicates(topic_domain, existing_preds)
            schema_count = len(DOMAIN_PREDICATE_SCHEMAS.get(topic_domain, []))
            signals['domain'] = 1.0
            signals['missing_schema_predicates'] = float(len(missing))
            if missing:
                missing_frac = len(missing) / max(schema_count, 1)
                qualitative_weight = 0.5 + missing_frac * 0.5
                if qualitative_weight > schema_weight:
                    schema_weight = qualitative_weight
                schema_matched = True
        else:
            signals['domain'] = 0.0
            signals['missing_schema_predicates'] = 0.0
    else:
        signals['domain'] = 0.0
        signals['missing_schema_predicates'] = 0.0

    if schema_matched:
        matched.append((InsufficiencyType.MISSING_SCHEMA, schema_weight))

    # ── Rule 7: DOMAIN_BOUNDARY_COLLAPSE ─────────────────────────────────────
    # High entropy + many source prefixes → topic is too broad / misclassified
    if domain_entropy > _BOUNDARY_MIN_ENTROPY and prefix_count >= _BOUNDARY_MIN_PREFIXES:
        weight = (domain_entropy - _BOUNDARY_MIN_ENTROPY) / (1.0 - _BOUNDARY_MIN_ENTROPY) * 0.5 + \
                 min(1.0, (prefix_count - _BOUNDARY_MIN_PREFIXES) / 5.0) * 0.5
        matched.append((InsufficiencyType.DOMAIN_BOUNDARY_COLLAPSE, weight))

    # ── Rule 8: SEMANTIC_INCOHERENCE ─────────────────────────────────────────
    # Validation Layer 2 severity above threshold → values are internally inconsistent
    if _HAS_VALIDATION:
        try:
            sem_report = validate_semantics(topic, db_conn)
            signals['semantic_severity'] = round(sem_report.severity, 4)
            if sem_report.severity > _INCOHERENCE_SEVERITY_THRESHOLD:
                weight = (sem_report.severity - _INCOHERENCE_SEVERITY_THRESHOLD) / \
                         (1.0 - _INCOHERENCE_SEVERITY_THRESHOLD)
                matched.append((InsufficiencyType.SEMANTIC_INCOHERENCE, round(weight, 4)))
        except Exception:
            signals['semantic_severity'] = 0.0
    else:
        signals['semantic_severity'] = 0.0

    # ── Rule 9: CROSS_TOPIC_DRIFT ─────────────────────────────────────────────
    # Validation Layer 3 severity above threshold → cross-topic asymmetry or org outlier
    if _HAS_VALIDATION:
        try:
            cross_report = validate_cross_topic(topic, db_conn)
            signals['cross_topic_severity'] = round(cross_report.severity, 4)
            if cross_report.severity > _CROSS_TOPIC_SEVERITY_THRESHOLD:
                weight = (cross_report.severity - _CROSS_TOPIC_SEVERITY_THRESHOLD) / \
                         (1.0 - _CROSS_TOPIC_SEVERITY_THRESHOLD)
                matched.append((InsufficiencyType.CROSS_TOPIC_DRIFT, round(weight, 4)))
        except Exception:
            signals['cross_topic_severity'] = 0.0
    else:
        signals['cross_topic_severity'] = 0.0

    # ── Assemble diagnosis ────────────────────────────────────────────────────
    total_rules = 9
    if not matched:
        types = [InsufficiencyType.UNKNOWN]
        confidence = 0.0
        matched_rules = 0
    else:
        # Sort by confidence weight descending (most confident first)
        matched.sort(key=lambda x: x[1], reverse=True)
        types = [t for t, _ in matched]
        # Overall confidence: mean of matched weights, scaled by match rate
        mean_weight = sum(w for _, w in matched) / len(matched)
        match_rate = len(matched) / total_rules
        confidence = min(1.0, mean_weight * (1.0 + match_rate * 0.3))
        matched_rules = len(matched)

    diagnosis = InsufficiencyDiagnosis(
        topic=topic,
        types=types,
        signals={k: round(float(v), 4) for k, v in signals.items()},
        confidence=round(confidence, 4),
        matched_rules=matched_rules,
        total_rules=total_rules,
    )

    print(f"[KBClassifier] {diagnosis.debug_str()}")
    return diagnosis
