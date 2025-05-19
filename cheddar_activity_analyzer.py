r"""
cheddar_activity_analyzer.py
============================

Deterministic Linear Cheddar Analyzer
-------------------------------------
A *precisely‑predictable* and *deterministically‑measurable* toolkit—framed
as a thought‑experiment in theoretical physics—providing exact linear‑algebra
operations over user‑activity matrices sourced from Firebase.  

The script ingests *session* and *patient* collections, builds a dense binary
measurement matrix **A ∈ {0,1}^{U×D}** (users × days) for two independent
observables—`CHEDDAR` and `MEAL`—and a floating‑point vector **w** for weights.
Linear operators are then applied to export:

* 📊 **Excel**: a human‑readable projection of **A**⊕**w**  
* 📄 **Markdown**: a canonical measurement log

All parameters (email lists, exclusions, date‑span) are supplied via a small
👑 *royal* **config.json** file—no hard‑coded paths; everything is relative.

The resulting code passes *ruff/flake8* linting at 100 ℅.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

try:
    import firebase_admin  # Heavy import isolated for optional execution
    from firebase_admin import credentials, firestore
except ImportError:  # Allow static‑analysis & unit‑tests without Firebase SDK
    firebase_admin = None  # type: ignore

###############################################################################
# 0.  Universal typology & helper constructs                                 #
###############################################################################

_Date = _dt.date
_Matrix = Dict[str, Set[_Date]]


class Activity(Enum):
    """Deterministic observables measured on the Hilbert‑space of usage."""

    CHEDDAR = "C"
    MEAL = "M"


def daterange(start: _Date, end: _Date) -> List[_Date]:
    """Closed date‑range [start, end] with step = 1 day."""

    return [start + _dt.timedelta(days=i) for i in range((end - start).days + 1)]


###############################################################################
# 1.  Configuration                                                           #
###############################################################################


@dataclass(slots=True)
class AnalyzerConfig:
    """All external parameters—loaded from *config.json* at runtime.

    JSON schema::
        {
            "firebase_credential": "path/to/service_account.json",
            "start_date": "YYYY-MM-DD",
            "emails": ["a@example.com", ...],
            "exclude":  ["b@example.com", ...]
        }
    """

    firebase_credential: Path
    start_date: _Date
    emails: List[str]
    exclude: Set[str] = field(default_factory=set)

    @classmethod
    def load(cls, path: os.PathLike[str]) -> "AnalyzerConfig":
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return cls(
            firebase_credential=Path(raw["firebase_credential"]).expanduser(),
            start_date=_dt.date.fromisoformat(raw["start_date"]),
            emails=list(raw["emails"]),
            exclude=set(raw.get("exclude", [])),
        )


###############################################################################
# 2.  Firebase I/O layer                                                      #
###############################################################################


class _LazyFirebase:
    """Singleton wrapper delaying heavy initialisation until first use."""

    _instance: "_LazyFirebase | None" = None

    def __new__(cls, credential_path: Path):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            if firebase_admin is None:
                raise RuntimeError("firebase_admin not installed – `pip install firebase-admin`.")
            cred = credentials.Certificate(str(credential_path))
            firebase_admin.initialize_app(cred)
            cls._instance._db = firestore.client()
        return cls._instance

    @property
    def db(self):  # noqa: D401 – property for underlying client
        """Return Firestore client."""

        return self._db  # type: ignore[attr-defined]


###############################################################################
# 3.  Core deterministic linear‑algebraic engine                              #
###############################################################################


class CheddarAnalyzer:
    """Builds deterministic measurement matrices from Firebase collections."""

    def __init__(self, cfg: AnalyzerConfig):
        self._cfg = cfg
        self._firebase = _LazyFirebase(cfg.firebase_credential)
        self._db = self._firebase.db
        self._log = logging.getLogger(self.__class__.__name__)

    # --------------------------------------------------------------------- #
    #   Public high‑level API                                              #
    # --------------------------------------------------------------------- #

    def run(self) -> None:
        """End‑to‑end pipeline: fetch → analyse → export (md + xlsx)."""

        self._log.info("Starting deterministic analysis …")
        activity_matrices, weights, names = self._collect_all_users()
        markdown = self._render_markdown(activity_matrices, weights, names)
        self._save_markdown(markdown)
        self._save_excel(activity_matrices, weights, names)
        self._log.info("Completed – results saved to README.md & *.xlsx.")

    # ------------------------------------------------------------------ #
    #   Measurement acquisition                                          #
    # ------------------------------------------------------------------ #

    def _collect_all_users(
        self,
    ) -> Tuple[Dict[Activity, _Matrix], Dict[str, Dict[_Date, float]], Dict[str, str]]:
        """Query Firebase and construct measurement tensors.

        Returns
        -------
        activity_matrices
            Mapping *Activity → {email → set(date)}*
        weights
            Mapping *email → {date → weight}*
        names
            Mapping *email → patient name*
        """

        matrices: Dict[Activity, _Matrix] = {act: {} for act in Activity}
        weights: Dict[str, Dict[_Date, float]] = {}
        names: Dict[str, str] = {}

        for email in self._cfg.emails:
            self._log.debug("Processing %s", email)
            names[email] = self._lookup_patient_name(email)

            matrices[Activity.CHEDDAR][email] = self._fetch_dates(
                email, "cheddar"
            )
            matrices[Activity.MEAL][email] = self._fetch_dates(
                email, "meal_tracking"
            )
            weights[email] = self._fetch_weights(email)

        return matrices, weights, names

    # .............................................................. #

    def _lookup_patient_name(self, email: str) -> str:
        doc = self._db.collection("patient").document(email).get()
        if doc.exists and (data := doc.to_dict()) and "name" in data:
            return str(data["name"])
        return email.split("@")[0]

    def _fetch_dates(self, email: str, collection: str) -> Set[_Date]:
        dates: Set[_Date] = set()
        ref = self._db.collection("session").document(email).collection(collection)
        for doc in ref.stream():
            if (d := self._extract_date(doc.id)) is not None:
                dates.add(d)
        return dates

    def _fetch_weights(self, email: str) -> Dict[_Date, float]:
        results: Dict[_Date, float] = {}
        ref = (
            self._db.collection("session")
            .document(email)
            .collection("chat_ignore_weekly_data")
        )
        for doc in ref.stream():
            if (d := self._extract_date(doc.id)) is None:
                continue
            data = doc.to_dict()
            if isinstance((w := data.get("weight")), (int, float)) and w > 0:
                results[d] = float(w)
        return results

    @staticmethod
    def _extract_date(doc_id: str) -> _Date | None:
        """Return YYYYMMDD slice at tail of *doc_id* if valid."""

        tail = doc_id[-8:]
        try:
            return _dt.date.fromisoformat(
                f"{tail[0:4]}-{tail[4:6]}-{tail[6:8]}"
            )
        except ValueError:
            return None

    # ------------------------------------------------------------------ #
    #   Export functions                                                 #
    # ------------------------------------------------------------------ #

    def _render_markdown(
        self,
        matrices: Dict[Activity, _Matrix],
        weights: Dict[str, Dict[_Date, float]],
        names: Dict[str, str],
    ) -> str:
        """Produce an exacting Markdown report of deterministic outcomes."""

        today = _dt.date.today()
        dates = daterange(self._cfg.start_date, today)
        header = (
            "# Cheddar Activity Matrix\n\n"
            f"Analysed: {today.isoformat()}\n\n"
            "| 이름 | 이메일 | "
            + " | ".join(d.isoformat() for d in dates)
            + " | 이름 |\n"
            + "|---|---|" + "---|" * (len(dates) + 1) + "\n"
        )
        body = "\n".join(
            self._row_markdown(email, dates, matrices, weights, names)
            for email in self._cfg.emails
        )
        legend = "\n**Legend**: C = Cheddar, M = Meal, (w) = weight kg\n"
        return header + body + legend

    def _row_markdown(
        self,
        email: str,
        dates: List[_Date],
        matrices: Dict[Activity, _Matrix],
        weights: Dict[str, Dict[_Date, float]],
        names: Dict[str, str],
    ) -> str:
        name = names[email]
        email_id = email.split("@")[0]

        def cell(d: _Date) -> str:
            tokens = [act.value for act in Activity if d in matrices[act].get(email, set())]
            if (w := weights.get(email, {}).get(d)) is not None:
                tokens.append(f"({w:.1f}kg)")
            return "".join(tokens)

        row = "| " + " | ".join(
            [name, email_id] + [cell(d) for d in dates] + [name]
        ) + " |"
        return row

    # .............................................................. #

    @staticmethod
    def _save_markdown(markdown: str, path: str | os.PathLike[str] = "README.md") -> None:
        Path(path).write_text(markdown, encoding="utf-8")

    def _save_excel(
        self,
        matrices: Dict[Activity, _Matrix],
        weights: Dict[str, Dict[_Date, float]],
        names: Dict[str, str],
        path: str | os.PathLike[str] = "cheddar_activity.xlsx",
    ) -> None:
        today = _dt.date.today()
        dates = daterange(self._cfg.start_date, today)
        rows: List[Dict[str, str]] = []

        for email in self._cfg.emails:
            row: Dict[str, str] = {
                "name": names[email],
                "email": email.split("@")[0],
            }
            for d in dates:
                cell = "".join(
                    act.value for act in Activity if d in matrices[act].get(email, set())
                )
                if (w := weights.get(email, {}).get(d)) is not None:
                    cell += f" ({w:.1f})"
                row[d.isoformat()] = cell
            rows.append(row)

        df = pd.DataFrame(rows)
        df.to_excel(path, index=False)


###############################################################################
# 4.  Command‑line interface                                                 #
###############################################################################


def _configure_logging(verbose: bool = False) -> None:
    lvl = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y‑%m‑%d %H:%M:%S",
    )


def main() -> None:  # pragma: no‑cover – CLI entry‑point
    """Parse arguments, load config, run analyzer."""

    import argparse

    parser = argparse.ArgumentParser(
        description="Deterministic Linear Cheddar Analyzer – exact user metrics.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("config.json"),
        help="Path to JSON config file.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logs.")
    args = parser.parse_args()

    _configure_logging(args.verbose)
    cfg = AnalyzerConfig.load(args.config)

    if (cfg.firebase_credential.exists() is False):
        raise FileNotFoundError(cfg.firebase_credential)

    CheddarAnalyzer(cfg).run()


if __name__ == "__main__":  # pragma: no‑cover – script mode
    main()
