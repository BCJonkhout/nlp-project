import os
import sqlite3
from typing import Optional, Dict, Any


class EvalStore:
    def __init__(self, db_path: str = "./evaluations.sqlite"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True) if os.path.dirname(db_path) else None

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def init(self):
        with self._conn() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS evaluations (
                    evaluation_id TEXT PRIMARY KEY,
                    created_at TEXT,
                    question TEXT,
                    optionA_json TEXT,
                    optionB_json TEXT,
                    chosen_option TEXT,
                    chosen_method TEXT,
                    top_k INTEGER,
                    window_size INTEGER,
                    topic TEXT,
                    scenario TEXT,
                    has_scenario INTEGER
                )
                """
            )
            self._ensure_columns(con)

    @staticmethod
    def _ensure_columns(con: sqlite3.Connection):
        cur = con.execute("PRAGMA table_info(evaluations)")
        existing = {row[1] for row in cur.fetchall()}
        desired = {
            "topic": ("ALTER TABLE evaluations ADD COLUMN topic TEXT",),
            "scenario": ("ALTER TABLE evaluations ADD COLUMN scenario TEXT",),
            "has_scenario": ("ALTER TABLE evaluations ADD COLUMN has_scenario INTEGER",),
        }
        for column, ddl in desired.items():
            if column not in existing:
                con.execute(ddl[0])

    def create(self, record: Dict[str, Any]):
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO evaluations (
                    evaluation_id, created_at, question, optionA_json, optionB_json,
                    chosen_option, chosen_method, top_k, window_size, topic, scenario, has_scenario
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["evaluation_id"],
                    record.get("created_at"),
                    record.get("question"),
                    json_dumps(record.get("optionA")),
                    json_dumps(record.get("optionB")),
                    record.get("chosen_option"),
                    record.get("chosen_method"),
                    record.get("top_k"),
                    record.get("window_size"),
                    record.get("topic"),
                    record.get("scenario"),
                    1 if record.get("has_scenario") else 0,
                ),
            )

    def get(self, evaluation_id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as con:
            cur = con.execute(
                "SELECT evaluation_id, created_at, question, optionA_json, optionB_json, chosen_option, chosen_method, top_k, window_size, topic, scenario, has_scenario FROM evaluations WHERE evaluation_id = ?",
                (evaluation_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "evaluation_id": row[0],
                "created_at": row[1],
                "question": row[2],
                "optionA": json_loads(row[3]),
                "optionB": json_loads(row[4]),
                "chosen_option": row[5],
                "chosen_method": row[6],
                "top_k": row[7],
                "window_size": row[8],
                "topic": row[9],
                "scenario": row[10],
                "has_scenario": bool(row[11]) if row[11] is not None else None,
            }

    def update_choice(self, evaluation_id: str, chosen_option: str, chosen_method: str):
        with self._conn() as con:
            con.execute(
                "UPDATE evaluations SET chosen_option = ?, chosen_method = ? WHERE evaluation_id = ?",
                (chosen_option, chosen_method, evaluation_id),
            )


def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


def json_loads(s: str) -> Any:
    import json
    return json.loads(s) if s else None
