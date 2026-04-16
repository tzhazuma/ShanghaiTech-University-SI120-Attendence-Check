#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import secrets
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

import pandas as pd


BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
CONFUSABLE_TRANSLATION = str.maketrans(
    {
        "i": "1",
        "I": "1",
        "l": "1",
        "L": "1",
        "o": "0",
        "O": "0",
    }
)
SUPPORTED_TABLE_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}
DEFAULT_NAME_FILE_CANDIDATES = ["name.xlsx", "name.csv", "name.xls", "name.xlsm"]
LOG_LEVELS = {
    "quiet": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}
PASSWD_INDEX_RE = re.compile(r"^passwd(?P<index>\d+)$", re.IGNORECASE)
MASK_64 = (1 << 64) - 1
LOGGER = logging.getLogger("password_generator")


@dataclass(frozen=True)
class PasswordFormat:
    length: int
    alphabet: str
    min_distance: int


@dataclass(frozen=True)
class GenerationConfig:
    algorithm: str
    seed_text: str
    length: int
    alphabet: str
    min_distance: int
    workers: int
    max_attempts_per_row: int


@dataclass(frozen=True)
class CandidateTask:
    row_indices: tuple[int, ...]
    attempts: tuple[int, ...]
    algorithm: str
    seed_text: str
    length: int
    alphabet: str


@dataclass(frozen=True)
class PasswordFileValidation:
    path: Path
    index: int | None
    valid: bool
    reason: str
    password_count: int


@dataclass(frozen=True)
class ReferencePasswordRecord:
    password: str
    normalized_password: str
    source_file: str
    row_number: int


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "根据名单和已有密码格式生成新一轮签到密码，支持并行生成、自定义随机算法、seed、长度和安全距离约束。"
        )
    )
    parser.add_argument(
        "--search-dir",
        default=".",
        help="默认搜索目录。未显式提供 name/reference/output 时，会在这个目录里自动发现。默认当前目录。",
    )
    parser.add_argument("--name-file", help="名单 Excel/CSV 路径；不传时自动优先查找 name.xlsx。")
    parser.add_argument(
        "--reference-files",
        nargs="*",
        default=None,
        help=(
            "已有密码表路径列表，用于推断格式并与新密码保持安全距离。"
            "如果不传，会自动搜索目录下合法的 passwdN 表格文件。"
        ),
    )
    parser.add_argument(
        "--output-file",
        help="输出密码表路径；不传时按当前目录下最大的 passwdN 自动生成 passwd(N+1).xlsx。",
    )
    parser.add_argument(
        "--round",
        type=int,
        help="显式指定轮次编号。未提供 output-file 时，会优先生成 passwd<round>。",
    )
    parser.add_argument(
        "--output-format",
        choices=("auto", "xlsx", "csv"),
        default="auto",
        help="输出格式，默认 auto。",
    )
    parser.add_argument(
        "--metadata-file",
        help="元数据 JSON 输出路径；不传时默认写到 <output_stem>_metadata.json。",
    )
    parser.add_argument(
        "--issue-file",
        help="发放版输出路径；不传时默认写到 <output_stem>_issue.{xlsx|csv}，只保留学号和 password。",
    )
    parser.add_argument(
        "--issue-format",
        choices=("auto", "xlsx", "csv"),
        default="csv",
        help="发放版输出格式，默认 csv。",
    )
    parser.add_argument(
        "--no-issue-file",
        action="store_true",
        help="不生成发放版文件。",
    )
    parser.add_argument(
        "--algorithm",
        choices=("blake2-counter", "splitmix64", "xorshift64star"),
        default="blake2-counter",
        help="随机算法，默认 blake2-counter。",
    )
    parser.add_argument(
        "--seed",
        help="随机种子。可以是任意字符串；不传时自动生成安全随机 seed 并写入元数据。",
    )
    parser.add_argument(
        "--length",
        type=int,
        help="密码长度；不传时从 reference files 推断，若无法推断则默认 12。",
    )
    parser.add_argument(
        "--alphabet",
        help="字符集；不传时从 reference files 推断，若无法推断则默认 Base62。",
    )
    parser.add_argument(
        "--min-distance",
        type=int,
        help="归一化后允许的最小编辑距离；不传时从 reference files 推断，若无法推断则默认 6。",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并行进程数，默认 0 表示自动使用全部 CPU 核心。",
    )
    parser.add_argument(
        "--max-attempts-per-row",
        type=int,
        default=5000,
        help="每一行最多重试多少次，默认 5000。",
    )
    parser.add_argument(
        "--log-level",
        choices=("quiet", "info", "debug"),
        default="info",
        help="日志级别。",
    )
    return parser.parse_args(argv)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(level=LOG_LEVELS[log_level], format="[%(levelname)s] %(message)s")


def normalize_column_name(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


def normalize_password(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = re.sub(r"\s+", "", str(value).strip())
    return text.translate(CONFUSABLE_TRANSLATION).lower()


def normalize_student_id(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip().replace(" ", "")
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".", maxsplit=1)[0]
    return text


def resolve_user_path(path_text: str | None, search_dir: Path) -> Path | None:
    if path_text is None:
        return None
    path = Path(path_text)
    if path.is_absolute():
        return path
    return search_dir / path


def find_column(columns: Sequence[Any], candidate_groups: Sequence[Sequence[str]], label: str) -> Any:
    normalized = {column: normalize_column_name(column) for column in columns}
    for tokens in candidate_groups:
        matches = [column for column, norm in normalized.items() if all(token in norm for token in tokens)]
        if matches:
            return matches[0]
    raise ValueError(f"找不到 {label} 列，当前列名是: {list(columns)}")


def detect_roster_columns(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "student_id": find_column(df.columns, [["学号"], ["student", "id"]], "名单学号"),
        "name": find_column(df.columns, [["姓名"], ["名字"], ["name"]], "名单姓名"),
        "email": find_column(df.columns, [["邮箱"], ["email"]], "名单邮箱"),
    }


def detect_password_column(df: pd.DataFrame) -> Any:
    return find_column(df.columns, [["password"], ["密码"]], "密码列")


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=object)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(path, dtype=object)
    raise ValueError(f"不支持的文件格式: {path}")


def resolve_output_mode(output_path: Path | None, output_format: str) -> str:
    if output_format != "auto":
        return output_format
    if output_path is None:
        return "xlsx"
    if output_path.suffix.lower() == ".csv":
        return "csv"
    return "xlsx"


def resolve_worker_count(requested_workers: int) -> int:
    if requested_workers < 0:
        raise ValueError("--workers 不能为负数")
    if requested_workers == 0:
        return max(1, os.cpu_count() or 1)
    return requested_workers


def validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} 必须大于 0")


def natural_sort_key(path: Path) -> list[Any]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", path.name)]


def extract_passwd_index(path: Path) -> int | None:
    matched = PASSWD_INDEX_RE.fullmatch(path.stem)
    if not matched:
        return None
    return int(matched.group("index"))


def discover_passwd_related_files(search_dir: Path) -> list[Path]:
    return sorted(
        [
            path
            for path in search_dir.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_TABLE_SUFFIXES and "passwd" in path.stem.lower()
        ],
        key=natural_sort_key,
    )


def inspect_password_file(path: Path) -> PasswordFileValidation:
    index = extract_passwd_index(path)
    if not path.exists():
        return PasswordFileValidation(path=path, index=index, valid=False, reason="文件不存在", password_count=0)
    try:
        df = read_table(path)
    except Exception as exc:
        return PasswordFileValidation(path=path, index=index, valid=False, reason=f"无法读取文件: {exc}", password_count=0)
    try:
        password_column = detect_password_column(df)
    except Exception as exc:
        return PasswordFileValidation(path=path, index=index, valid=False, reason=f"缺少密码列: {exc}", password_count=0)
    non_empty_passwords = [normalize_password(value) for value in df[password_column].tolist() if normalize_password(value)]
    if not non_empty_passwords:
        return PasswordFileValidation(path=path, index=index, valid=False, reason="密码列为空", password_count=0)
    return PasswordFileValidation(path=path, index=index, valid=True, reason="ok", password_count=len(non_empty_passwords))


def inspect_roster_file(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, "文件不存在"
    try:
        df = read_table(path)
        detect_roster_columns(df)
    except Exception as exc:
        return False, str(exc)
    return True, "ok"


def resolve_name_file(explicit_name_file: str | None, search_dir: Path) -> tuple[Path, bool]:
    explicit_path = resolve_user_path(explicit_name_file, search_dir)
    if explicit_path is not None:
        valid, reason = inspect_roster_file(explicit_path)
        if not valid:
            raise ValueError(f"名单文件不合法: {explicit_path}，原因: {reason}")
        return explicit_path, False

    for candidate_name in DEFAULT_NAME_FILE_CANDIDATES:
        candidate_path = search_dir / candidate_name
        valid, _ = inspect_roster_file(candidate_path)
        if valid:
            LOGGER.info("未显式提供 name-file，自动使用: %s", candidate_path)
            return candidate_path, True

    fallback_candidates = sorted(
        [
            path
            for path in search_dir.iterdir()
            if path.is_file() and path.suffix.lower() in SUPPORTED_TABLE_SUFFIXES and "name" in path.stem.lower()
        ],
        key=natural_sort_key,
    )
    for candidate_path in fallback_candidates:
        valid, reason = inspect_roster_file(candidate_path)
        if valid:
            LOGGER.info("未找到默认 name.xlsx，回退使用: %s", candidate_path)
            return candidate_path, True
        LOGGER.debug("跳过不合法名单候选 %s: %s", candidate_path, reason)

    raise ValueError(
        f"未找到合法的名单文件。请在 {search_dir} 中提供 name.xlsx/name.csv，或显式传入 --name-file"
    )


def resolve_reference_files(
    explicit_files: Sequence[str] | None,
    search_dir: Path,
) -> tuple[list[Path], list[PasswordFileValidation], bool]:
    if explicit_files is not None:
        if len(explicit_files) == 0:
            return [], [], False
        reference_paths: list[Path] = []
        validations: list[PasswordFileValidation] = []
        for file_text in explicit_files:
            path = resolve_user_path(file_text, search_dir)
            assert path is not None
            validation = inspect_password_file(path)
            validations.append(validation)
            if not validation.valid:
                raise ValueError(f"参考密码文件不合法: {path}，原因: {validation.reason}")
            reference_paths.append(path)
        return reference_paths, validations, False

    validations: list[PasswordFileValidation] = []
    reference_paths: list[Path] = []
    for candidate in discover_passwd_related_files(search_dir):
        validation = inspect_password_file(candidate)
        validations.append(validation)
        if validation.index is None:
            LOGGER.debug("跳过非 canonical passwd 文件: %s", candidate)
            continue
        if not validation.valid:
            LOGGER.warning("跳过不合法的参考密码文件 %s: %s", candidate, validation.reason)
            continue
        reference_paths.append(candidate)

    LOGGER.info("自动发现合法参考密码文件: %s", ", ".join(path.name for path in reference_paths) or "none")
    return reference_paths, validations, True


def collect_reference_password_records(reference_files: Sequence[Path]) -> list[ReferencePasswordRecord]:
    records: list[ReferencePasswordRecord] = []
    for path in reference_files:
        df = read_table(path)
        password_column = detect_password_column(df)
        for row_index, value in enumerate(df[password_column].tolist(), start=2):
            normalized_password = normalize_password(value)
            if not normalized_password:
                continue
            records.append(
                ReferencePasswordRecord(
                    password=str(value),
                    normalized_password=normalized_password,
                    source_file=path.name,
                    row_number=row_index,
                )
            )
    return records


def collect_reference_passwords(reference_files: Sequence[Path]) -> list[str]:
    return [record.password for record in collect_reference_password_records(reference_files)]


def infer_length(passwords: Sequence[str]) -> int:
    if not passwords:
        return 12
    counter = Counter(len(password) for password in passwords if password)
    if not counter:
        return 12
    return counter.most_common(1)[0][0]


def infer_alphabet(passwords: Sequence[str]) -> str:
    characters = sorted(set("".join(passwords)))
    return "".join(characters) if characters else BASE62_ALPHABET


def limited_levenshtein(left: str, right: str, max_distance: int) -> int:
    if left == right:
        return 0
    if max_distance < 0:
        return max_distance + 1
    left_length = len(left)
    right_length = len(right)
    if abs(left_length - right_length) > max_distance:
        return max_distance + 1
    if left_length < right_length:
        left, right = right, left
        left_length, right_length = right_length, left_length

    previous_row = list(range(right_length + 1))
    for left_index, left_char in enumerate(left, start=1):
        window_start = max(1, left_index - max_distance)
        window_end = min(right_length, left_index + max_distance)
        current_row = [max_distance + 1] * (right_length + 1)
        current_row[0] = left_index
        for right_index in range(window_start, window_end + 1):
            replacement_cost = 0 if left_char == right[right_index - 1] else 1
            current_row[right_index] = min(
                previous_row[right_index] + 1,
                current_row[right_index - 1] + 1,
                previous_row[right_index - 1] + replacement_cost,
            )
        previous_row = current_row
        if min(previous_row) > max_distance:
            return max_distance + 1
    return previous_row[right_length]


def compute_min_normalized_distance(passwords: Sequence[str]) -> int | None:
    normalized = [normalize_password(password) for password in passwords if normalize_password(password)]
    if len(normalized) < 2:
        return None
    min_distance: int | None = None
    for left_index, left in enumerate(normalized):
        for right in normalized[left_index + 1 :]:
            distance = limited_levenshtein(left, right, max_distance=max(len(left), len(right)))
            if min_distance is None or distance < min_distance:
                min_distance = distance
    return min_distance


def infer_min_distance(passwords: Sequence[str]) -> int:
    inferred = compute_min_normalized_distance(passwords)
    if inferred is None:
        return 6
    return max(3, inferred)


def infer_next_passwd_index(discovered_password_file_validations: Sequence[PasswordFileValidation]) -> int:
    indexes = [validation.index for validation in discovered_password_file_validations if validation.index is not None]
    return (max(indexes) + 1) if indexes else 1


def resolve_target_round(
    explicit_round: int | None,
    output_file_text: str | None,
    discovered_password_file_validations: Sequence[PasswordFileValidation],
) -> int:
    if explicit_round is not None:
        validate_positive("round", explicit_round)
        return explicit_round
    if output_file_text is not None:
        output_index = extract_passwd_index(Path(output_file_text))
        if output_index is not None:
            return output_index
    return infer_next_passwd_index(discovered_password_file_validations)


def resolve_seed(seed: str | None) -> tuple[str, bool]:
    if seed is not None:
        return seed, False
    return secrets.token_hex(16), True


def build_password_format(
    *,
    reference_passwords: Sequence[str],
    length: int | None,
    alphabet: str | None,
    min_distance: int | None,
) -> PasswordFormat:
    resolved_length = length if length is not None else infer_length(reference_passwords)
    resolved_alphabet = alphabet if alphabet is not None else infer_alphabet(reference_passwords)
    resolved_min_distance = min_distance if min_distance is not None else infer_min_distance(reference_passwords)
    validate_positive("密码长度", resolved_length)
    validate_positive("最小编辑距离", resolved_min_distance)
    if len(set(resolved_alphabet)) != len(resolved_alphabet):
        raise ValueError("alphabet 中不能有重复字符")
    if len(resolved_alphabet) < 2:
        raise ValueError("alphabet 至少需要两个不同字符")
    return PasswordFormat(length=resolved_length, alphabet=resolved_alphabet, min_distance=resolved_min_distance)


def seed_material(seed_text: str, *, row_index: int, attempt: int, tag: str) -> bytes:
    return f"{seed_text}|{row_index}|{attempt}|{tag}".encode("utf-8")


def derive_u64(seed_text: str, *, row_index: int, attempt: int, tag: str) -> int:
    digest = hashlib.blake2b(
        seed_material(seed_text, row_index=row_index, attempt=attempt, tag=tag),
        digest_size=16,
        person=b"si120-seed-v1",
    ).digest()
    value = int.from_bytes(digest[:8], byteorder="little") & MASK_64
    return value or 0x9E3779B97F4A7C15


def splitmix64_next(state: int) -> tuple[int, int]:
    state = (state + 0x9E3779B97F4A7C15) & MASK_64
    value = state
    value ^= value >> 30
    value = (value * 0xBF58476D1CE4E5B9) & MASK_64
    value ^= value >> 27
    value = (value * 0x94D049BB133111EB) & MASK_64
    value ^= value >> 31
    return state, value & MASK_64


def xorshift64star_next(state: int) -> tuple[int, int]:
    state ^= state >> 12
    state &= MASK_64
    state ^= (state << 25) & MASK_64
    state &= MASK_64
    state ^= state >> 27
    state &= MASK_64
    value = (state * 2685821657736338717) & MASK_64
    return state, value


def iter_random_bytes_blake2(seed_text: str, *, row_index: int, attempt: int) -> Iterator[int]:
    counter = 0
    while True:
        digest = hashlib.blake2b(
            seed_material(seed_text, row_index=row_index, attempt=attempt, tag=f"b2-{counter}"),
            digest_size=32,
            person=b"si120-b2-v1",
        ).digest()
        for byte in digest:
            yield byte
        counter += 1


def iter_random_bytes_splitmix64(seed_text: str, *, row_index: int, attempt: int) -> Iterator[int]:
    state = derive_u64(seed_text, row_index=row_index, attempt=attempt, tag="splitmix64")
    while True:
        state, value = splitmix64_next(state)
        for byte in value.to_bytes(8, byteorder="little"):
            yield byte


def iter_random_bytes_xorshift64star(seed_text: str, *, row_index: int, attempt: int) -> Iterator[int]:
    state = derive_u64(seed_text, row_index=row_index, attempt=attempt, tag="xorshift64star")
    while True:
        state, value = xorshift64star_next(state)
        for byte in value.to_bytes(8, byteorder="little"):
            yield byte


def iter_random_bytes(algorithm: str, seed_text: str, *, row_index: int, attempt: int) -> Iterator[int]:
    if algorithm == "blake2-counter":
        return iter_random_bytes_blake2(seed_text, row_index=row_index, attempt=attempt)
    if algorithm == "splitmix64":
        return iter_random_bytes_splitmix64(seed_text, row_index=row_index, attempt=attempt)
    if algorithm == "xorshift64star":
        return iter_random_bytes_xorshift64star(seed_text, row_index=row_index, attempt=attempt)
    raise ValueError(f"不支持的随机算法: {algorithm}")


def generate_password(
    *,
    algorithm: str,
    seed_text: str,
    row_index: int,
    attempt: int,
    length: int,
    alphabet: str,
) -> str:
    byte_stream = iter_random_bytes(algorithm, seed_text, row_index=row_index, attempt=attempt)
    limit = 256 - (256 % len(alphabet))
    password_chars: list[str] = []
    while len(password_chars) < length:
        byte = next(byte_stream)
        if byte >= limit:
            continue
        password_chars.append(alphabet[byte % len(alphabet)])
    return "".join(password_chars)


def chunk_row_indices(row_indices: Sequence[int], workers: int) -> list[tuple[int, ...]]:
    if not row_indices:
        return []
    chunk_size = max(1, (len(row_indices) + (workers * 2) - 1) // (workers * 2))
    return [tuple(row_indices[start : start + chunk_size]) for start in range(0, len(row_indices), chunk_size)]


def generate_candidate_chunk(task: CandidateTask) -> list[tuple[int, str]]:
    generated: list[tuple[int, str]] = []
    for row_index, attempt in zip(task.row_indices, task.attempts):
        generated.append(
            (
                row_index,
                generate_password(
                    algorithm=task.algorithm,
                    seed_text=task.seed_text,
                    row_index=row_index,
                    attempt=attempt,
                    length=task.length,
                    alphabet=task.alphabet,
                ),
            )
        )
    return generated


def generate_candidates_parallel(
    row_indices: Sequence[int],
    attempts: dict[int, int],
    config: GenerationConfig,
) -> dict[int, str]:
    tasks = [
        CandidateTask(
            row_indices=chunk,
            attempts=tuple(attempts[row_index] for row_index in chunk),
            algorithm=config.algorithm,
            seed_text=config.seed_text,
            length=config.length,
            alphabet=config.alphabet,
        )
        for chunk in chunk_row_indices(row_indices, config.workers)
    ]

    if config.workers == 1:
        task_results = map(generate_candidate_chunk, tasks)
    else:
        with ProcessPoolExecutor(max_workers=config.workers) as executor:
            task_results = executor.map(generate_candidate_chunk, tasks)

    generated: dict[int, str] = {}
    for chunk_result in task_results:
        for row_index, password in chunk_result:
            generated[row_index] = password
    return generated


def is_password_far_enough(candidate: str, existing: Sequence[str], min_distance: int) -> bool:
    if min_distance <= 0:
        return True
    max_distance = min_distance - 1
    for other in existing:
        if limited_levenshtein(candidate, other, max_distance=max_distance) <= max_distance:
            return False
    return True


def generate_passwords_for_roster(
    roster_df: pd.DataFrame,
    config: GenerationConfig,
    reference_passwords: Sequence[str],
) -> tuple[list[str], dict[str, Any]]:
    reference_normalized = [normalize_password(password) for password in reference_passwords if normalize_password(password)]
    accepted_passwords: dict[int, str] = {}
    accepted_normalized: list[str] = []
    attempts = {row_index: 0 for row_index in range(len(roster_df))}
    pending = list(range(len(roster_df)))
    pass_count = 0

    while pending:
        pass_count += 1
        candidates = generate_candidates_parallel(pending, attempts, config)
        next_pending: list[int] = []
        accepted_in_pass = 0
        for row_index in pending:
            candidate_password = candidates[row_index]
            candidate_normalized = normalize_password(candidate_password)
            if is_password_far_enough(candidate_normalized, reference_normalized, config.min_distance) and is_password_far_enough(candidate_normalized, accepted_normalized, config.min_distance):
                accepted_passwords[row_index] = candidate_password
                accepted_normalized.append(candidate_normalized)
                accepted_in_pass += 1
            else:
                attempts[row_index] += 1
                if attempts[row_index] > config.max_attempts_per_row:
                    raise RuntimeError(
                        f"第 {row_index + 1} 行超过最大重试次数 {config.max_attempts_per_row}，请增大 length 或调整 alphabet/min-distance"
                    )
                next_pending.append(row_index)
        LOGGER.debug("生成轮次 %s: accepted=%s retried=%s", pass_count, accepted_in_pass, len(next_pending))
        pending = next_pending

    ordered_passwords = [accepted_passwords[row_index] for row_index in range(len(roster_df))]
    metadata = {
        "generation_passes": pass_count,
        "max_attempt_used": max(attempts.values(), default=0),
        "total_retries": sum(attempts.values()),
        "generated_min_normalized_distance": compute_min_normalized_distance(ordered_passwords),
        "reference_min_normalized_distance": compute_min_normalized_distance(reference_passwords),
    }
    return ordered_passwords, metadata


def build_output_dataframe(roster_df: pd.DataFrame, passwords: Sequence[str]) -> pd.DataFrame:
    output_df = roster_df.copy()
    output_df["password"] = list(passwords)
    return output_df


def build_issue_dataframe(roster_df: pd.DataFrame, roster_columns: dict[str, Any], passwords: Sequence[str]) -> pd.DataFrame:
    student_id_column = roster_columns["student_id"]
    return pd.DataFrame(
        {
            str(student_id_column): roster_df[student_id_column].map(normalize_student_id),
            "password": list(passwords),
        }
    )


def resolve_default_output_path(
    search_dir: Path,
    output_format: str,
    target_round: int,
) -> Path:
    suffix = ".csv" if output_format == "csv" else ".xlsx"
    path = search_dir / f"passwd{target_round}{suffix}"
    LOGGER.info("未显式提供 output-file，自动使用: %s", path)
    return path


def resolve_output_path(
    output_file_text: str | None,
    search_dir: Path,
    output_format: str,
    target_round: int,
) -> tuple[Path, bool]:
    explicit_path = resolve_user_path(output_file_text, search_dir)
    resolved_mode = resolve_output_mode(explicit_path, output_format)
    if explicit_path is None:
        return resolve_default_output_path(search_dir, resolved_mode, target_round), True
    if resolved_mode == "csv":
        return (explicit_path if explicit_path.suffix.lower() == ".csv" else explicit_path.with_suffix(".csv")), False
    return (explicit_path if explicit_path.suffix.lower() == ".xlsx" else explicit_path.with_suffix(".xlsx")), False


def resolve_metadata_path(output_path: Path, metadata_file: str | None, search_dir: Path) -> Path:
    explicit_path = resolve_user_path(metadata_file, search_dir)
    if explicit_path is not None:
        return explicit_path
    return output_path.with_name(f"{output_path.stem}_metadata.json")


def resolve_issue_path(
    output_path: Path,
    issue_file: str | None,
    issue_format: str,
    search_dir: Path,
    disabled: bool,
) -> tuple[Path | None, bool]:
    if disabled:
        return None, False
    explicit_path = resolve_user_path(issue_file, search_dir)
    if issue_format == "auto" and explicit_path is None:
        resolved_issue_format = "csv" if output_path.suffix.lower() == ".csv" else "xlsx"
    else:
        resolved_issue_format = resolve_output_mode(explicit_path, issue_format)
    if explicit_path is not None:
        if resolved_issue_format == "csv":
            return (explicit_path if explicit_path.suffix.lower() == ".csv" else explicit_path.with_suffix(".csv")), False
        return (explicit_path if explicit_path.suffix.lower() == ".xlsx" else explicit_path.with_suffix(".xlsx")), False
    suffix = ".csv" if resolved_issue_format == "csv" else ".xlsx"
    return output_path.with_name(f"{output_path.stem}_issue{suffix}"), True


def build_nearest_reference_examples(
    generated_passwords: Sequence[str],
    reference_records: Sequence[ReferencePasswordRecord],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    if not generated_passwords or not reference_records:
        return []
    examples: list[dict[str, Any]] = []
    for row_index, generated_password in enumerate(generated_passwords, start=2):
        normalized_generated = normalize_password(generated_password)
        best_record: ReferencePasswordRecord | None = None
        best_distance: int | None = None
        for reference_record in reference_records:
            distance = limited_levenshtein(
                normalized_generated,
                reference_record.normalized_password,
                max_distance=max(len(normalized_generated), len(reference_record.normalized_password)),
            )
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_record = reference_record
        if best_record is None or best_distance is None:
            continue
        examples.append(
            {
                "generated_row_number": row_index,
                "generated_password": generated_password,
                "reference_password": best_record.password,
                "reference_source_file": best_record.source_file,
                "reference_row_number": best_record.row_number,
                "normalized_distance": best_distance,
            }
        )
    examples.sort(key=lambda item: (item["normalized_distance"], item["generated_row_number"]))
    return examples[:limit]


def build_nearest_generated_examples(generated_passwords: Sequence[str], *, limit: int = 5) -> list[dict[str, Any]]:
    if len(generated_passwords) < 2:
        return []
    examples: list[dict[str, Any]] = []
    normalized_passwords = [normalize_password(password) for password in generated_passwords]
    for left_index, left in enumerate(normalized_passwords):
        for right_index in range(left_index + 1, len(normalized_passwords)):
            right = normalized_passwords[right_index]
            distance = limited_levenshtein(left, right, max_distance=max(len(left), len(right)))
            examples.append(
                {
                    "left_row_number": left_index + 2,
                    "left_password": generated_passwords[left_index],
                    "right_row_number": right_index + 2,
                    "right_password": generated_passwords[right_index],
                    "normalized_distance": distance,
                }
            )
    examples.sort(key=lambda item: (item["normalized_distance"], item["left_row_number"], item["right_row_number"]))
    return examples[:limit]


def write_table(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
    else:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Sheet1", index=False)
    return path


def write_metadata(metadata: dict[str, Any], metadata_path: Path) -> Path:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata_path


def build_metadata(
    *,
    search_dir: Path,
    target_round: int,
    name_file: Path,
    auto_name_file_used: bool,
    output_path: Path,
    auto_output_file_used: bool,
    metadata_path: Path,
    issue_path: Path | None,
    auto_issue_file_used: bool,
    issue_format: str,
    roster_df: pd.DataFrame,
    reference_files: Sequence[Path],
    reference_validations: Sequence[PasswordFileValidation],
    reference_records: Sequence[ReferencePasswordRecord],
    auto_reference_discovery_used: bool,
    auto_seed_generated: bool,
    config: GenerationConfig,
    format_spec: PasswordFormat,
    generation_stats: dict[str, Any],
    nearest_reference_examples: Sequence[dict[str, Any]],
    nearest_generated_examples: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    skipped_reference_files = [
        {
            "path": str(validation.path),
            "reason": validation.reason,
            "index": validation.index,
        }
        for validation in reference_validations
        if validation.index is not None and not validation.valid
    ]
    return {
        "generated_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "search_dir": str(search_dir),
        "target_round": target_round,
        "name_file": str(name_file),
        "auto_name_file_used": auto_name_file_used,
        "output_file": str(output_path),
        "auto_output_file_used": auto_output_file_used,
        "metadata_file": str(metadata_path),
        "issue_file": None if issue_path is None else str(issue_path),
        "auto_issue_file_used": auto_issue_file_used,
        "issue_format": issue_format,
        "row_count": len(roster_df),
        "reference_files": [str(path) for path in reference_files],
        "reference_file_count": len(reference_files),
        "reference_password_count": len(reference_records),
        "auto_reference_discovery_used": auto_reference_discovery_used,
        "skipped_reference_files": skipped_reference_files,
        "seed": config.seed_text,
        "auto_seed_generated": auto_seed_generated,
        "algorithm": config.algorithm,
        "workers": config.workers,
        "max_attempts_per_row": config.max_attempts_per_row,
        "length": format_spec.length,
        "alphabet": format_spec.alphabet,
        "alphabet_size": len(format_spec.alphabet),
        "min_distance": format_spec.min_distance,
        "closest_reference_examples": list(nearest_reference_examples),
        "closest_generated_examples": list(nearest_generated_examples),
        **generation_stats,
    }


def summarize_reference_files(reference_files: Sequence[Path]) -> str:
    if not reference_files:
        return "none"
    return ", ".join(path.name for path in reference_files)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    search_dir = Path(args.search_dir).resolve()
    workers = resolve_worker_count(args.workers)
    validate_positive("max_attempts_per_row", args.max_attempts_per_row)

    name_file, auto_name_file_used = resolve_name_file(args.name_file, search_dir)
    roster_df = read_table(name_file)
    roster_columns = detect_roster_columns(roster_df)
    LOGGER.info("读取名单成功: %s 行", len(roster_df))

    reference_files, reference_validations, auto_reference_discovery_used = resolve_reference_files(
        args.reference_files,
        search_dir,
    )
    LOGGER.info("参考密码文件: %s", summarize_reference_files(reference_files))
    reference_records = collect_reference_password_records(reference_files)
    reference_passwords = [record.password for record in reference_records]
    LOGGER.info("参考密码总数: %s", len(reference_passwords))

    target_round = resolve_target_round(args.round, args.output_file, reference_validations)
    LOGGER.info("目标轮次: %s", target_round)

    output_path, auto_output_file_used = resolve_output_path(
        args.output_file,
        search_dir,
        args.output_format,
        target_round,
    )
    metadata_path = resolve_metadata_path(output_path, args.metadata_file, search_dir)
    issue_path, auto_issue_file_used = resolve_issue_path(
        output_path=output_path,
        issue_file=args.issue_file,
        issue_format=args.issue_format,
        search_dir=search_dir,
        disabled=args.no_issue_file,
    )

    format_spec = build_password_format(
        reference_passwords=reference_passwords,
        length=args.length,
        alphabet=args.alphabet,
        min_distance=args.min_distance,
    )
    seed_text, auto_seed_generated = resolve_seed(args.seed)
    config = GenerationConfig(
        algorithm=args.algorithm,
        seed_text=seed_text,
        length=format_spec.length,
        alphabet=format_spec.alphabet,
        min_distance=format_spec.min_distance,
        workers=workers,
        max_attempts_per_row=args.max_attempts_per_row,
    )
    LOGGER.info(
        "生成配置: algorithm=%s length=%s alphabet_size=%s min_distance=%s workers=%s",
        config.algorithm,
        config.length,
        len(config.alphabet),
        config.min_distance,
        config.workers,
    )
    if auto_seed_generated:
        LOGGER.info("未显式提供 seed，已自动生成安全 seed")
    LOGGER.info("实际使用 seed: %s", config.seed_text)

    passwords, generation_stats = generate_passwords_for_roster(
        roster_df=roster_df,
        config=config,
        reference_passwords=reference_passwords,
    )
    nearest_reference_examples = build_nearest_reference_examples(passwords, reference_records)
    nearest_generated_examples = build_nearest_generated_examples(passwords)
    output_df = build_output_dataframe(roster_df, passwords)
    issue_df = build_issue_dataframe(roster_df, roster_columns, passwords)

    resolved_output_path = write_table(output_df, output_path)
    resolved_issue_path = write_table(issue_df, issue_path) if issue_path is not None else None
    metadata = build_metadata(
        search_dir=search_dir,
        target_round=target_round,
        name_file=name_file,
        auto_name_file_used=auto_name_file_used,
        output_path=resolved_output_path,
        auto_output_file_used=auto_output_file_used,
        metadata_path=metadata_path,
        issue_path=resolved_issue_path,
        auto_issue_file_used=auto_issue_file_used,
        issue_format=args.issue_format,
        roster_df=roster_df,
        reference_files=reference_files,
        reference_validations=reference_validations,
        reference_records=reference_records,
        auto_reference_discovery_used=auto_reference_discovery_used,
        auto_seed_generated=auto_seed_generated,
        config=config,
        format_spec=format_spec,
        generation_stats=generation_stats,
        nearest_reference_examples=nearest_reference_examples,
        nearest_generated_examples=nearest_generated_examples,
    )
    resolved_metadata_path = write_metadata(metadata, metadata_path)

    print(f"输出密码文件: {resolved_output_path}")
    if resolved_issue_path is not None:
        print(f"输出发放版文件: {resolved_issue_path}")
    print(f"输出元数据文件: {resolved_metadata_path}")
    print(f"目标轮次: {target_round}")
    print(f"密码数量: {len(passwords)}")
    print(f"默认长度: {config.length}")
    print(f"最小归一化编辑距离约束: {config.min_distance}")
    print(f"随机算法: {config.algorithm}")
    print(f"实际 seed: {config.seed_text}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"错误: {exc}", file=sys.stderr)
        raise SystemExit(1)