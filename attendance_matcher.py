#!/usr/bin/env python3

from __future__ import annotations

import argparse
import itertools
import json
import logging
import math
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd


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


@dataclass(frozen=True)
class RoundWindow:
    index: int
    label: str
    start: pd.Timestamp
    end: pd.Timestamp | None
    source: str


@dataclass(frozen=True)
class PasswordSet:
    provided_position: int
    path: Path
    label: str
    canonical_passwords: tuple[str, ...]
    canonical_to_original: dict[str, str]


@dataclass(frozen=True)
class MatchTask:
    round_index: int
    passwords: tuple[str, ...]
    max_distance: int
    rows: tuple[tuple[int, str], ...]


ROUND_LABELS = {
    1: "第1次签到",
    2: "第2次签到",
    3: "第3次签到",
    4: "第4次签到",
}

ROUND_COUNT = 4

EXCEPTION_STATUS_DESCRIPTIONS = {
    "duplicate_password": "多个不同学生命中了同一个规范密码，本轮全部记 0 分。",
    "unmatched_password": "提交密码在允许的模糊匹配阈值内找不到任何候选密码。",
    "ambiguous_password": "提交密码在允许阈值内同时命中多个候选密码，无法唯一判定。",
    "unresolved_student": "无法把提交记录解析到名单中的唯一学生。",
    "outside_window": "提交时间不在任何生效的时间窗口内。",
}

LOG_LEVEL_NAMES = {
    "quiet": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

LOGGER = logging.getLogger("attendance_matcher")


def default_round_label(index: int) -> str:
    return ROUND_LABELS.get(index, f"第{index}次签到")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "根据名单、签到密码和问卷结果生成签到得分，支持密码模糊匹配、重复密码判零、"
            "CSV/Excel 输入输出、可配置时间窗口和默认启用的自适应窗口划分。"
        )
    )
    parser.add_argument("--name-file", required=True, help="名单 Excel/CSV 路径，例如 name.xlsx 或 name.csv")
    parser.add_argument(
        "--password-files",
        nargs=ROUND_COUNT,
        required=True,
        metavar=("PASSWD1", "PASSWD2", "PASSWD3", "PASSWD4"),
        help="四份密码 Excel/CSV 路径，按提供顺序传入。",
    )
    parser.add_argument("--result-file", required=True, help="签到结果 Excel/CSV 路径")
    parser.add_argument("--output-file", required=True, help="输出结果路径；可配合 --output-format 控制输出格式")
    parser.add_argument(
        "--output-format",
        choices=("auto", "xlsx", "csv"),
        default="auto",
        help="输出格式，默认 auto。auto 会根据输出文件扩展名选择；.csv 为 CSV 模式，否则为 xlsx。",
    )
    parser.add_argument(
        "--password-order",
        nargs=ROUND_COUNT,
        type=int,
        metavar=("R1", "R2", "R3", "R4"),
        help=(
            "手动指定每一轮对应的是第几个密码文件。"
            "例如传入的密码文件是 passwd1 passwd2 passwd3 passwd4，"
            "若想使用 1,3,2,4 的映射，则写 --password-order 1 3 2 4。"
        ),
    )
    parser.add_argument(
        "--max-distance",
        type=int,
        default=2,
        help="允许的最大编辑距离，默认 2。",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并行进程数，默认 0 表示自动使用全部 CPU 核心。",
    )
    parser.add_argument(
        "--window-mode",
        choices=("adaptive", "official", "manual", "file"),
        default="adaptive",
        help=(
            "时间窗口模式。adaptive 会根据提交日期自动划分 4 个窗口并默认启用；"
            "official 使用内置固定窗口；manual 使用 --time-window 显式配置；"
            "file 从配置文件加载窗口。"
        ),
    )
    parser.add_argument(
        "--time-window",
        action="append",
        default=None,
        help=(
            "手动配置一个时间窗口，仅在 --window-mode manual 时使用。"
            "格式为 START,END[,LABEL]，例如 2026-03-27,2026-03-31,第2次签到；"
            "END 可写 open 表示开区间。"
        ),
    )
    parser.add_argument(
        "--window-config-file",
        help=(
            "时间窗口配置文件路径，仅在 --window-mode file 时使用。"
            "支持 JSON、CSV，也兼容 Excel 表格。"
        ),
    )
    parser.add_argument(
        "--log-level",
        choices=("quiet", "info", "debug"),
        default="info",
        help="日志级别。quiet 仅输出警告，info 输出主要推断信息，debug 输出详细推断过程。",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="显式指定签到年份；不传时会从签到结果时间列自动推断。",
    )
    return parser.parse_args()


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=LOG_LEVEL_NAMES[log_level],
        format="[%(levelname)s] %(message)s",
    )


def validate_window_arguments(args: argparse.Namespace, round_count: int) -> None:
    if args.window_mode == "manual":
        if args.window_config_file:
            raise ValueError("--window-mode manual 不能同时使用 --window-config-file")
        if not args.time_window or len(args.time_window) != round_count:
            raise ValueError(
                f"--window-mode manual 必须提供恰好 {round_count} 个 --time-window 参数"
            )
        return

    if args.time_window:
        raise ValueError("只有 --window-mode manual 时才能使用 --time-window")

    if args.window_mode == "file":
        if not args.window_config_file:
            raise ValueError("--window-mode file 时必须提供 --window-config-file")
        return

    if args.window_config_file:
        raise ValueError("只有 --window-mode file 时才能使用 --window-config-file")


def normalize_column_name(value: Any) -> str:
    text = str(value or "")
    return re.sub(r"\s+", "", text).lower()


def normalize_name(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", "", text).lower()


def normalize_student_id(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip().replace(" ", "")
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".", maxsplit=1)[0]
    return text


def normalize_password(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", "", text)
    return text.translate(CONFUSABLE_TRANSLATION).lower()


def find_column(columns: Sequence[Any], candidate_groups: Sequence[Sequence[str]], label: str) -> Any:
    normalized = {column: normalize_column_name(column) for column in columns}
    for tokens in candidate_groups:
        matches = [column for column, norm in normalized.items() if all(token in norm for token in tokens)]
        if matches:
            return matches[0]
    raise ValueError(f"找不到 {label} 列，当前列名是: {list(columns)}")


def read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=object)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(path, dtype=object)
    raise ValueError(f"不支持的表格格式: {path}. 目前支持 .csv, .xlsx, .xls, .xlsm")


def find_optional_column(columns: Sequence[Any], candidate_groups: Sequence[Sequence[str]]) -> Any | None:
    try:
        return find_column(columns, candidate_groups, "optional")
    except ValueError:
        return None


def get_record_field(record: dict[str, Any], candidate_names: Sequence[str], default: Any = None) -> Any:
    normalized_to_key = {normalize_column_name(key): key for key in record}
    for candidate_name in candidate_names:
        normalized_name = normalize_column_name(candidate_name)
        if normalized_name in normalized_to_key:
            return record[normalized_to_key[normalized_name]]
    return default


def normalize_window_text(value: str, default_year: int) -> str:
    text = value.strip()
    if re.fullmatch(r"\d{1,2}[/-]\d{1,2}", text):
        return f"{default_year}-{text.replace('/', '-') }"
    return text


def is_date_only_text(value: str) -> bool:
    return re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", value) is not None


def parse_window_boundary(value: str, *, default_year: int, is_end: bool) -> pd.Timestamp | None:
    text = str(value or "").strip()
    if not text:
        raise ValueError("时间窗口边界不能为空")
    if text.lower() in {"open", "none", "null", "inf"}:
        if is_end:
            return None
        raise ValueError("时间窗口开始时间不能是 open")

    normalized_text = normalize_window_text(text, default_year=default_year)
    try:
        timestamp = pd.Timestamp(normalized_text)
    except Exception as exc:
        raise ValueError(f"无法解析时间窗口边界: {value!r}") from exc

    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    if is_end and is_date_only_text(normalized_text):
        timestamp = timestamp + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return timestamp


def format_window_boundary(timestamp: pd.Timestamp | None) -> str:
    if timestamp is None:
        return "open"
    if timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0 and timestamp.microsecond == 0:
        return timestamp.strftime("%Y-%m-%d")
    if (
        timestamp.hour == 23
        and timestamp.minute == 59
        and timestamp.second == 59
        and timestamp.microsecond == 999999
    ):
        return timestamp.strftime("%Y-%m-%d")
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def validate_round_windows(round_windows: Sequence[RoundWindow], round_count: int) -> None:
    if len(round_windows) != round_count:
        raise ValueError(f"时间窗口数量必须是 {round_count}，当前为 {len(round_windows)}")
    labels = [round_window.label for round_window in round_windows]
    if len(set(labels)) != len(labels):
        raise ValueError(f"时间窗口标签不能重复，当前标签为: {labels}")

    for round_index, round_window in enumerate(round_windows):
        if round_window.end is not None and round_window.end < round_window.start:
            raise ValueError(f"时间窗口结束时间不能早于开始时间: {round_window.label}")
        if round_window.end is None and round_index != len(round_windows) - 1:
            raise ValueError(f"只有最后一个时间窗口允许使用 open 结束时间: {round_window.label}")
        if round_index == 0:
            continue
        previous_window = round_windows[round_index - 1]
        if previous_window.end is None or previous_window.end >= round_window.start:
            raise ValueError(
                f"时间窗口发生重叠或顺序错误: {previous_window.label} 与 {round_window.label}"
            )


def parse_window_records_to_round_windows(
    records: Sequence[dict[str, Any]],
    *,
    default_year: int,
    round_count: int,
    source: str,
) -> list[RoundWindow]:
    if len(records) != round_count:
        raise ValueError(f"窗口配置文件中必须有 {round_count} 条窗口记录，当前为 {len(records)}")

    round_windows: list[RoundWindow] = []
    for index, record in enumerate(records, start=1):
        start_value = get_record_field(record, ["start", "开始", "start_time", "window_start", "开始时间"])
        end_value = get_record_field(record, ["end", "结束", "end_time", "window_end", "结束时间"])
        if start_value is None or end_value is None:
            raise ValueError(f"窗口配置第 {index} 条记录缺少 start/end 字段: {record}")
        label_value = get_record_field(record, ["label", "标签", "name", "轮次", "title"], default_round_label(index))
        round_windows.append(
            RoundWindow(
                index=index,
                label=str(label_value).strip() or default_round_label(index),
                start=parse_window_boundary(str(start_value), default_year=default_year, is_end=False),
                end=parse_window_boundary(str(end_value), default_year=default_year, is_end=True),
                source=source,
            )
        )

    validate_round_windows(round_windows, round_count=round_count)
    return round_windows


def build_file_round_windows(
    config_path: Path,
    *,
    default_year: int,
    round_count: int,
) -> list[RoundWindow]:
    suffix = config_path.suffix.lower()
    LOGGER.info("从窗口配置文件加载时间窗口: %s", config_path)
    if suffix == ".json":
        with config_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            records = payload.get("windows") or payload.get("round_windows") or payload.get("items")
        elif isinstance(payload, list):
            records = payload
        else:
            raise ValueError(f"无法解析窗口配置 JSON: {config_path}")
        if not isinstance(records, list):
            raise ValueError(f"窗口配置 JSON 必须是数组，或包含 windows 数组字段: {config_path}")
        return parse_window_records_to_round_windows(
            records=records,
            default_year=default_year,
            round_count=round_count,
            source="file",
        )

    table = read_table(config_path)
    start_column = find_column(table.columns, [["start"], ["开始"]], "窗口开始")
    end_column = find_column(table.columns, [["end"], ["结束"]], "窗口结束")
    label_column = find_optional_column(table.columns, [["label"], ["标签"], ["name"], ["轮次"]])
    records = []
    for _, row in table.iterrows():
        record = {
            "start": row[start_column],
            "end": row[end_column],
        }
        if label_column is not None:
            record["label"] = row[label_column]
        records.append(record)
    return parse_window_records_to_round_windows(
        records=records,
        default_year=default_year,
        round_count=round_count,
        source="file",
    )


def build_official_round_windows(year: int, round_count: int) -> list[RoundWindow]:
    if round_count != ROUND_COUNT:
        raise ValueError(f"official 模式当前仅支持 {ROUND_COUNT} 轮签到")
    round_windows = [
        RoundWindow(
            index=1,
            label=default_round_label(1),
            start=pd.Timestamp(year=year, month=1, day=1),
            end=pd.Timestamp(year=year, month=3, day=26, hour=23, minute=59, second=59, microsecond=999999),
            source="official",
        ),
        RoundWindow(
            index=2,
            label=default_round_label(2),
            start=pd.Timestamp(year=year, month=3, day=27),
            end=pd.Timestamp(year=year, month=3, day=31, hour=23, minute=59, second=59, microsecond=999999),
            source="official",
        ),
        RoundWindow(
            index=3,
            label=default_round_label(3),
            start=pd.Timestamp(year=year, month=4, day=1),
            end=pd.Timestamp(year=year, month=4, day=3, hour=23, minute=59, second=59, microsecond=999999),
            source="official",
        ),
        RoundWindow(
            index=4,
            label=default_round_label(4),
            start=pd.Timestamp(year=year, month=4, day=15),
            end=None,
            source="official",
        ),
    ]
    validate_round_windows(round_windows, round_count=round_count)
    LOGGER.info("采用 official 固定时间窗口")
    return round_windows


def build_manual_round_windows(
    window_specs: Sequence[str] | None,
    *,
    default_year: int,
    round_count: int,
) -> list[RoundWindow]:
    if not window_specs or len(window_specs) != round_count:
        raise ValueError(
            f"manual 模式必须提供恰好 {round_count} 个 --time-window 参数，当前为 {0 if window_specs is None else len(window_specs)}"
        )

    round_windows: list[RoundWindow] = []
    for index, window_spec in enumerate(window_specs, start=1):
        parts = [part.strip() for part in str(window_spec).split(",")]
        if len(parts) < 2:
            raise ValueError(
                f"无法解析 --time-window {window_spec!r}，格式应为 START,END[,LABEL]"
            )
        start_text = parts[0]
        end_text = parts[1]
        label = ",".join(parts[2:]).strip() or default_round_label(index)
        round_windows.append(
            RoundWindow(
                index=index,
                label=label,
                start=parse_window_boundary(start_text, default_year=default_year, is_end=False),
                end=parse_window_boundary(end_text, default_year=default_year, is_end=True),
                source="manual",
            )
        )

    validate_round_windows(round_windows, round_count=round_count)
    LOGGER.info("采用 manual 时间窗口，共 %s 个", len(round_windows))
    return round_windows


def build_adaptive_round_windows(submitted_at: pd.Series, round_count: int) -> list[RoundWindow]:
    active_dates = sorted({timestamp.normalize() for timestamp in submitted_at.dropna().tolist()})
    LOGGER.debug(
        "自适应窗口活跃日期: %s",
        [active_date.strftime("%Y-%m-%d") for active_date in active_dates],
    )
    if len(active_dates) < round_count:
        raise ValueError(
            f"活跃日期只有 {len(active_dates)} 个，少于需要划分的 {round_count} 个窗口"
        )

    if round_count == 1:
        split_indices: list[int] = []
    else:
        gap_indices = list(range(len(active_dates) - 1))
        LOGGER.debug(
            "自适应窗口日期间隔: %s",
            [
                {
                    "left": active_dates[index].strftime("%Y-%m-%d"),
                    "right": active_dates[index + 1].strftime("%Y-%m-%d"),
                    "gap_days": (active_dates[index + 1] - active_dates[index]).days,
                }
                for index in gap_indices
            ],
        )
        gap_indices.sort(
            key=lambda index: (
                (active_dates[index + 1] - active_dates[index]).days,
                int(active_dates[index].value),
            ),
            reverse=True,
        )
        split_indices = sorted(gap_indices[: round_count - 1])
        LOGGER.debug("自适应窗口选中的切分点索引: %s", split_indices)

    round_windows: list[RoundWindow] = []
    cluster_start_index = 0
    for round_index, cluster_end_index in enumerate(split_indices + [len(active_dates) - 1], start=1):
        cluster_dates = active_dates[cluster_start_index : cluster_end_index + 1]
        round_windows.append(
            RoundWindow(
                index=round_index,
                label=default_round_label(round_index),
                start=cluster_dates[0],
                end=cluster_dates[-1] + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1),
                source="adaptive",
            )
        )
        cluster_start_index = cluster_end_index + 1

    validate_round_windows(round_windows, round_count=round_count)
    LOGGER.info(
        "采用 adaptive 时间窗口: %s",
        [
            f"{round_window.label}={format_window_boundary(round_window.start)}~{format_window_boundary(round_window.end)}"
            for round_window in round_windows
        ],
    )
    return round_windows


def resolve_round_windows(
    *,
    window_mode: str,
    submitted_at: pd.Series,
    year: int,
    manual_specs: Sequence[str] | None,
    window_config_file: Path | None,
    round_count: int,
) -> tuple[list[RoundWindow], str | None]:
    if window_mode == "official":
        return build_official_round_windows(year=year, round_count=round_count), None
    if window_mode == "manual":
        return build_manual_round_windows(
            window_specs=manual_specs,
            default_year=year,
            round_count=round_count,
        ), None
    if window_mode == "file":
        if window_config_file is None:
            raise ValueError("window_mode=file 时必须提供窗口配置文件")
        return build_file_round_windows(
            config_path=window_config_file,
            default_year=year,
            round_count=round_count,
        ), f"窗口配置文件: {window_config_file}"

    try:
        return build_adaptive_round_windows(submitted_at=submitted_at, round_count=round_count), None
    except ValueError as exc:
        fallback_windows = build_official_round_windows(year=year, round_count=round_count)
        LOGGER.warning("自适应窗口划分失败，回退到 official 模式: %s", exc)
        return fallback_windows, f"自适应窗口划分失败，已回退到 official 模式: {exc}"


def summarize_active_dates(round_df: pd.DataFrame, time_column: str) -> str:
    active_dates = sorted({timestamp.normalize() for timestamp in round_df[time_column].dropna().tolist()})
    return ", ".join(date.strftime("%Y-%m-%d") for date in active_dates)


def detect_roster_columns(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "student_id": find_column(df.columns, [["学号"], ["student", "id"]], "名单学号"),
        "name": find_column(df.columns, [["姓名"], ["名字"], ["name"]], "名单姓名"),
        "email": find_column(df.columns, [["邮箱"], ["email"]], "名单邮箱"),
    }


def detect_result_columns(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "submitted_at": find_column(df.columns, [["提交答卷时间"], ["提交", "时间"], ["submit", "time"]], "提交时间"),
        "student_id": find_column(df.columns, [["学号"], ["student", "id"]], "结果学号"),
        "name": find_column(df.columns, [["名字"], ["姓名"], ["name"]], "结果姓名"),
        "password": find_column(df.columns, [["密码"], ["password"]], "结果密码"),
    }


def detect_password_column(df: pd.DataFrame) -> Any:
    return find_column(df.columns, [["password"], ["密码"]], "密码表 password")


def limited_levenshtein(left: str, right: str, max_distance: int) -> int:
    if left == right:
        return 0
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


def match_one_password(submitted_password: str, canonical_passwords: Sequence[str], max_distance: int) -> dict[str, Any]:
    matches: list[tuple[str, int]] = []
    for canonical_password in canonical_passwords:
        distance = limited_levenshtein(submitted_password, canonical_password, max_distance=max_distance)
        if distance <= max_distance:
            matches.append((canonical_password, distance))
            if len(matches) > 1:
                break

    if not matches:
        return {
            "match_status": "unmatched_password",
            "matched_password": "",
            "matched_distance": None,
        }
    if len(matches) > 1:
        return {
            "match_status": "ambiguous_password",
            "matched_password": "",
            "matched_distance": None,
        }

    matched_password, matched_distance = matches[0]
    return {
        "match_status": "matched_password",
        "matched_password": matched_password,
        "matched_distance": matched_distance,
    }


def match_chunk(task: MatchTask) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for submission_id, normalized_password in task.rows:
        match_result = match_one_password(
            submitted_password=normalized_password,
            canonical_passwords=task.passwords,
            max_distance=task.max_distance,
        )
        results.append(
            {
                "submission_id": submission_id,
                "round_index": task.round_index,
                **match_result,
            }
        )
    return results


def resolve_worker_count(requested_workers: int) -> int:
    if requested_workers < 0:
        raise ValueError("--workers 不能为负数")
    if requested_workers == 0:
        return max(1, os.cpu_count() or 1)
    return requested_workers


def infer_year(submitted_at: pd.Series, explicit_year: int | None) -> int:
    if explicit_year is not None:
        return explicit_year
    years = submitted_at.dropna().dt.year
    if years.empty:
        raise ValueError("提交时间列为空，无法推断年份")
    return int(years.mode().iloc[0])


def load_password_sets(paths: Sequence[Path]) -> list[PasswordSet]:
    password_sets: list[PasswordSet] = []
    for provided_position, path in enumerate(paths, start=1):
        password_df = read_table(path)
        password_column = detect_password_column(password_df)
        canonical_to_original: dict[str, str] = {}
        for raw_password in password_df[password_column].tolist():
            canonical_password = normalize_password(raw_password)
            if not canonical_password:
                continue
            if canonical_password in canonical_to_original:
                raise ValueError(
                    f"密码表 {path} 在归一化后出现重复密码: {canonical_to_original[canonical_password]!r} 和 {raw_password!r}"
                )
            canonical_to_original[canonical_password] = str(raw_password)
        password_sets.append(
            PasswordSet(
                provided_position=provided_position,
                path=path,
                label=path.stem,
                canonical_passwords=tuple(canonical_to_original.keys()),
                canonical_to_original=canonical_to_original,
            )
        )
    return password_sets


def score_round_against_password_set(
    round_df: pd.DataFrame,
    password_set: PasswordSet,
    password_column: str,
    max_distance: int,
) -> int:
    total = 0
    for raw_password in round_df[password_column].tolist():
        normalized_password = normalize_password(raw_password)
        match_result = match_one_password(normalized_password, password_set.canonical_passwords, max_distance=max_distance)
        if match_result["match_status"] == "matched_password":
            total += 1
    return total


def choose_password_mapping(
    round_windows: Sequence[RoundWindow],
    round_frames: dict[int, pd.DataFrame],
    password_sets: Sequence[PasswordSet],
    result_password_column: str,
    result_time_column: str,
    max_distance: int,
    manual_order: Sequence[int] | None,
) -> tuple[list[int], pd.DataFrame]:
    score_rows: list[dict[str, Any]] = []
    score_matrix: list[list[int]] = []
    for round_window in round_windows:
        round_df = round_frames[round_window.index]
        row_scores: list[int] = []
        for password_set in password_sets:
            matched_rows = score_round_against_password_set(
                round_df=round_df,
                password_set=password_set,
                password_column=result_password_column,
                max_distance=max_distance,
            )
            row_scores.append(matched_rows)
            score_rows.append(
                {
                    "轮次": round_window.label,
                    "窗口来源": round_window.source,
                    "窗口开始": format_window_boundary(round_window.start),
                    "窗口结束": format_window_boundary(round_window.end),
                    "窗口活跃日期": summarize_active_dates(round_df, result_time_column),
                    "窗口提交数": len(round_df),
                    "候选密码文件顺位": password_set.provided_position,
                    "候选密码文件": password_set.path.name,
                    "命中提交数": matched_rows,
                }
            )
        score_matrix.append(row_scores)

    for round_position, round_window in enumerate(round_windows, start=1):
        LOGGER.debug(
            "密码映射候选命中数 %s: %s",
            round_window.label,
            {
                password_sets[index].path.name: score_matrix[round_position - 1][index]
                for index in range(len(password_sets))
            },
        )

    if manual_order is not None:
        mapping = [position - 1 for position in manual_order]
        LOGGER.info("采用手动密码文件顺序: %s", list(manual_order))
    else:
        permutations = list(itertools.permutations(range(len(password_sets))))
        permutation_scores = [
            (
                permutation,
                sum(
                    score_matrix[round_index][password_index]
                    for round_index, password_index in enumerate(permutation)
                ),
            )
            for permutation in permutations
        ]
        LOGGER.debug(
            "密码映射候选总分 Top5: %s",
            [
                {
                    "order": [index + 1 for index in permutation],
                    "score": score,
                }
                for permutation, score in sorted(
                    permutation_scores,
                    key=lambda item: (item[1], item[0]),
                    reverse=True,
                )[:5]
            ],
        )
        mapping = list(
            max(
                permutation_scores,
                key=lambda item: item[1],
            )
        )
        mapping = list(mapping[0]) if mapping and isinstance(mapping[0], tuple) else mapping

    LOGGER.info(
        "采用的密码映射顺序: %s",
        [
            {
                "round": round_windows[round_index].label,
                "password_file_order": password_sets[password_index].provided_position,
                "password_file": password_sets[password_index].path.name,
            }
            for round_index, password_index in enumerate(mapping)
        ],
    )

    summary_rows: list[dict[str, Any]] = []
    for round_position, password_index in enumerate(mapping, start=1):
        round_window = round_windows[round_position - 1]
        round_df = round_frames[round_window.index]
        summary_row: dict[str, Any] = {
            "轮次": round_window.label,
            "窗口来源": round_window.source,
            "窗口开始": format_window_boundary(round_window.start),
            "窗口结束": format_window_boundary(round_window.end),
            "窗口活跃日期": summarize_active_dates(round_df, result_time_column),
            "窗口提交数": len(round_df),
            "采用的密码文件顺位": password_sets[password_index].provided_position,
            "采用的密码文件": password_sets[password_index].path.name,
            "采用映射后的命中提交数": score_matrix[round_position - 1][password_index],
        }
        for candidate_index, password_set in enumerate(password_sets, start=1):
            summary_row[f"候选{candidate_index}:{password_set.path.name}"] = score_matrix[round_position - 1][candidate_index - 1]
        summary_rows.append(summary_row)

    mapping_df = pd.DataFrame(summary_rows)
    raw_score_df = pd.DataFrame(score_rows)
    mapping_df = pd.concat([mapping_df, pd.DataFrame([{}]), raw_score_df], ignore_index=True)
    return mapping, mapping_df


def build_roster_lookup(roster_df: pd.DataFrame, roster_columns: dict[str, Any]) -> tuple[dict[str, int], dict[str, int]]:
    student_id_to_row: dict[str, int] = {}
    name_to_rows: defaultdict[str, list[int]] = defaultdict(list)
    for row_index, row in roster_df.iterrows():
        student_id = normalize_student_id(row[roster_columns["student_id"]])
        student_name = normalize_name(row[roster_columns["name"]])
        if student_id:
            student_id_to_row[student_id] = row_index
        if student_name:
            name_to_rows[student_name].append(row_index)
    unique_name_to_row = {name: row_indexes[0] for name, row_indexes in name_to_rows.items() if len(row_indexes) == 1}
    return student_id_to_row, unique_name_to_row


def resolve_student(
    roster_df: pd.DataFrame,
    roster_columns: dict[str, Any],
    student_id_to_row: dict[str, int],
    unique_name_to_row: dict[str, int],
    submitted_student_id: str,
    submitted_name: str,
) -> tuple[int | None, str]:
    if submitted_student_id and submitted_student_id in student_id_to_row:
        roster_row = student_id_to_row[submitted_student_id]
        roster_name = normalize_name(roster_df.at[roster_row, roster_columns["name"]])
        if submitted_name and submitted_name != roster_name:
            return roster_row, "student_id_exact_name_mismatch"
        return roster_row, "student_id_exact"
    if submitted_name and submitted_name in student_id_to_row and submitted_student_id in unique_name_to_row:
        roster_row = unique_name_to_row[submitted_student_id]
        swapped_roster_name = normalize_name(roster_df.at[roster_row, roster_columns["name"]])
        swapped_roster_student_id = normalize_student_id(roster_df.at[roster_row, roster_columns["student_id"]])
        if swapped_roster_name == submitted_student_id and swapped_roster_student_id == submitted_name:
            return roster_row, "student_id_name_swapped"
    if submitted_name and submitted_name in unique_name_to_row:
        return unique_name_to_row[submitted_name], "name_exact_fallback"
    return None, "unresolved_student"


def chunk_rows(rows: Sequence[tuple[int, str]], chunk_count: int) -> list[tuple[tuple[int, str], ...]]:
    if not rows:
        return []
    chunk_size = max(1, math.ceil(len(rows) / chunk_count))
    return [tuple(rows[start : start + chunk_size]) for start in range(0, len(rows), chunk_size)]


def run_parallel_matching(
    round_rows: dict[int, list[tuple[int, str]]],
    password_sets_by_round: dict[int, PasswordSet],
    max_distance: int,
    workers: int,
) -> dict[int, dict[int, dict[str, Any]]]:
    tasks: list[MatchTask] = []
    for round_index, rows in round_rows.items():
        chunk_total = min(max(1, workers * 2), max(1, len(rows)))
        for chunk in chunk_rows(rows, chunk_total):
            tasks.append(
                MatchTask(
                    round_index=round_index,
                    passwords=password_sets_by_round[round_index].canonical_passwords,
                    max_distance=max_distance,
                    rows=chunk,
                )
            )

    round_matches: dict[int, dict[int, dict[str, Any]]] = {round_index: {} for round_index in round_rows}
    if not tasks:
        return round_matches

    if workers == 1:
        task_results = map(match_chunk, tasks)
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            task_results = executor.map(match_chunk, tasks)

    for chunk_result in task_results:
        for row_result in chunk_result:
            round_matches[row_result["round_index"]][row_result["submission_id"]] = row_result
    return round_matches


def build_submission_frames(
    result_df: pd.DataFrame,
    result_columns: dict[str, Any],
    round_windows: Sequence[RoundWindow],
) -> tuple[dict[int, pd.DataFrame], pd.DataFrame]:
    round_frames: dict[int, pd.DataFrame] = {}
    matched_indexes: set[int] = set()
    time_column = result_columns["submitted_at"]
    for round_window in round_windows:
        if round_window.end is None:
            mask = result_df[time_column] >= round_window.start
        else:
            mask = (result_df[time_column] >= round_window.start) & (result_df[time_column] <= round_window.end)
        round_frames[round_window.index] = result_df.loc[mask].copy()
        matched_indexes.update(result_df.index[mask].tolist())
    outside_df = result_df.loc[~result_df.index.isin(matched_indexes)].copy()
    return round_frames, outside_df


def build_score_output(
    roster_df: pd.DataFrame,
    roster_columns: dict[str, Any],
    round_windows: Sequence[RoundWindow],
    valid_student_ids_by_round: dict[int, set[str]],
) -> pd.DataFrame:
    output_df = roster_df.copy()
    roster_student_id_column = roster_columns["student_id"]
    for round_window in round_windows:
        output_df[round_window.label] = output_df[roster_student_id_column].map(
            lambda value, round_index=round_window.index: 1
            if normalize_student_id(value) in valid_student_ids_by_round[round_index]
            else 0
        )
    output_df["总分"] = output_df[[round_window.label for round_window in round_windows]].sum(axis=1)
    return output_df


def collect_round_details(
    round_window: RoundWindow,
    round_df: pd.DataFrame,
    round_matches: dict[int, dict[str, Any]],
    roster_df: pd.DataFrame,
    roster_columns: dict[str, Any],
    student_id_to_row: dict[str, int],
    unique_name_to_row: dict[str, int],
    result_columns: dict[str, Any],
    password_set: PasswordSet,
) -> tuple[list[dict[str, Any]], set[str]]:
    details: list[dict[str, Any]] = []
    valid_student_ids: set[str] = set()
    matched_password_to_students: defaultdict[str, set[str]] = defaultdict(set)
    matched_password_to_rows: defaultdict[str, list[int]] = defaultdict(list)

    for submission_id, row in round_df.iterrows():
        input_student_id = normalize_student_id(row[result_columns["student_id"]])
        input_name = normalize_name(row[result_columns["name"]])
        roster_row, resolution_method = resolve_student(
            roster_df=roster_df,
            roster_columns=roster_columns,
            student_id_to_row=student_id_to_row,
            unique_name_to_row=unique_name_to_row,
            submitted_student_id=input_student_id,
            submitted_name=input_name,
        )
        match_result = round_matches[submission_id]

        roster_student_id = ""
        roster_name = ""
        if roster_row is not None:
            roster_student_id = normalize_student_id(roster_df.at[roster_row, roster_columns["student_id"]])
            roster_name = str(roster_df.at[roster_row, roster_columns["name"]])

        detail = {
            "结果表行号": submission_id + 2,
            "轮次": round_window.label,
            "提交时间": row[result_columns["submitted_at"]],
            "输入学号": input_student_id,
            "输入姓名": str(row[result_columns["name"]] or ""),
            "解析到的学号": roster_student_id,
            "解析到的姓名": roster_name,
            "人员解析方式": resolution_method,
            "原始密码": str(row[result_columns["password"]] or ""),
            "归一化密码": normalize_password(row[result_columns["password"]]),
            "密码文件": password_set.path.name,
            "匹配状态": match_result["match_status"],
            "匹配到的规范密码": match_result["matched_password"],
            "匹配距离": match_result["matched_distance"],
            "重复人数": 0,
            "最终判定": "pending",
            "本轮得分": 0,
        }
        details.append(detail)

        if roster_student_id and match_result["match_status"] == "matched_password":
            matched_password_to_students[match_result["matched_password"]].add(roster_student_id)
            matched_password_to_rows[match_result["matched_password"]].append(len(details) - 1)

    duplicate_passwords = {
        matched_password: students
        for matched_password, students in matched_password_to_students.items()
        if len(students) > 1
    }

    for detail in details:
        matched_password = detail["匹配到的规范密码"]
        if detail["人员解析方式"] == "unresolved_student":
            detail["最终判定"] = "unresolved_student"
            continue
        if detail["匹配状态"] != "matched_password":
            detail["最终判定"] = detail["匹配状态"]
            continue
        if matched_password in duplicate_passwords:
            detail["最终判定"] = "duplicate_password"
            detail["重复人数"] = len(duplicate_passwords[matched_password])
            continue
        detail["最终判定"] = "valid_match"
        detail["本轮得分"] = 1
        valid_student_ids.add(detail["解析到的学号"])

    return details, valid_student_ids


def collect_outside_window_details(outside_df: pd.DataFrame, result_columns: dict[str, Any]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for submission_id, row in outside_df.iterrows():
        details.append(
            {
                "结果表行号": submission_id + 2,
                "轮次": "窗口外",
                "提交时间": row[result_columns["submitted_at"]],
                "输入学号": normalize_student_id(row[result_columns["student_id"]]),
                "输入姓名": str(row[result_columns["name"]] or ""),
                "解析到的学号": "",
                "解析到的姓名": "",
                "人员解析方式": "outside_window",
                "原始密码": str(row[result_columns["password"]] or ""),
                "归一化密码": normalize_password(row[result_columns["password"]]),
                "密码文件": "",
                "匹配状态": "outside_window",
                "匹配到的规范密码": "",
                "匹配距离": None,
                "重复人数": 0,
                "最终判定": "outside_window",
                "本轮得分": 0,
            }
        )
    return details


def build_exception_summary(details_df: pd.DataFrame) -> pd.DataFrame:
    exceptions_df = details_df[details_df["最终判定"] != "valid_match"].copy()
    columns = ["异常类型", "记录数", "涉及轮次", "涉及学生数", "结果表行号", "说明"]
    if exceptions_df.empty:
        return pd.DataFrame(columns=columns)

    summary_rows: list[dict[str, Any]] = []
    for status, group in exceptions_df.groupby("最终判定", sort=False):
        student_series = group["解析到的学号"].fillna("")
        fallback_student_series = group["输入学号"].fillna("")
        merged_student_ids = [
            normalize_student_id(resolved_student_id) or normalize_student_id(input_student_id)
            for resolved_student_id, input_student_id in zip(student_series.tolist(), fallback_student_series.tolist())
        ]
        distinct_student_ids = sorted({student_id for student_id in merged_student_ids if student_id})
        row_numbers = ", ".join(str(int(row_number)) for row_number in group["结果表行号"].tolist())
        rounds = ", ".join(dict.fromkeys(str(round_name) for round_name in group["轮次"].tolist()))
        summary_rows.append(
            {
                "异常类型": status,
                "记录数": len(group),
                "涉及轮次": rounds,
                "涉及学生数": len(distinct_student_ids),
                "结果表行号": row_numbers,
                "说明": EXCEPTION_STATUS_DESCRIPTIONS.get(status, "非正常匹配结果，请结合 details sheet 查看。"),
            }
        )

    order_map = {
        status: index
        for index, status in enumerate(
            [
                "duplicate_password",
                "unmatched_password",
                "ambiguous_password",
                "unresolved_student",
                "outside_window",
            ]
        )
    }
    summary_rows.sort(key=lambda row: order_map.get(row["异常类型"], 999))
    return pd.DataFrame(summary_rows, columns=columns)


def resolve_output_mode(output_path: Path, output_format: str) -> str:
    if output_format != "auto":
        return output_format
    return "csv" if output_path.suffix.lower() == ".csv" else "xlsx"


def resolve_output_paths(output_path: Path, output_format: str) -> dict[str, Path]:
    resolved_format = resolve_output_mode(output_path=output_path, output_format=output_format)
    if resolved_format == "csv":
        scores_path = output_path if output_path.suffix.lower() == ".csv" else output_path.with_suffix(".csv")
        base_stem = scores_path.stem
        return {
            "scores": scores_path,
            "details": scores_path.with_name(f"{base_stem}_details.csv"),
            "mapping": scores_path.with_name(f"{base_stem}_mapping.csv"),
            "exceptions": scores_path.with_name(f"{base_stem}_exceptions.csv"),
        }
    workbook_path = output_path if output_path.suffix.lower() == ".xlsx" else output_path.with_suffix(".xlsx")
    return {"workbook": workbook_path}


def write_output(
    output_path: Path,
    output_format: str,
    scores_df: pd.DataFrame,
    details_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    exceptions_df: pd.DataFrame,
) -> dict[str, Path]:
    output_paths = resolve_output_paths(output_path=output_path, output_format=output_format)
    for path in output_paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    if "workbook" in output_paths:
        with pd.ExcelWriter(output_paths["workbook"], engine="openpyxl") as writer:
            scores_df.to_excel(writer, sheet_name="scores", index=False)
            details_df.to_excel(writer, sheet_name="details", index=False)
            mapping_df.to_excel(writer, sheet_name="mapping", index=False)
            exceptions_df.to_excel(writer, sheet_name="exceptions", index=False)
        return output_paths

    scores_df.to_csv(output_paths["scores"], index=False)
    details_df.to_csv(output_paths["details"], index=False)
    mapping_df.to_csv(output_paths["mapping"], index=False)
    exceptions_df.to_csv(output_paths["exceptions"], index=False)
    return output_paths


def validate_manual_order(password_order: Sequence[int] | None, round_count: int) -> None:
    if password_order is None:
        return
    expected = set(range(1, round_count + 1))
    received = set(password_order)
    if len(password_order) != round_count or received != expected:
        raise ValueError(f"--password-order 必须恰好包含 1 到 {round_count} 各一次")


def print_summary(
    output_paths: dict[str, Path],
    round_windows: Sequence[RoundWindow],
    mapping_df: pd.DataFrame,
    scores_df: pd.DataFrame,
    workers: int,
    window_note: str | None,
) -> None:
    score_columns = [round_window.label for round_window in round_windows]
    print(f"使用并行进程数: {workers}")
    print(f"时间窗口模式: {round_windows[0].source if round_windows else 'unknown'}")
    if window_note:
        print(f"窗口说明: {window_note}")
    print("采用的时间窗口:")
    for round_window in round_windows:
        print(
            f"  {round_window.label}: {format_window_boundary(round_window.start)} ~ {format_window_boundary(round_window.end)}"
        )
    print("采用的轮次映射:")
    mapping_rows = mapping_df[
        mapping_df.get("采用的密码文件顺位", pd.Series(index=mapping_df.index, dtype=object)).notna()
    ]
    for _, row in mapping_rows.iterrows():
        print(
            f"  {row['轮次']}: 第{int(row['采用的密码文件顺位'])}个密码文件 ({row['采用的密码文件']})"
        )
    print("各轮得分人数:")
    for score_column in score_columns:
        print(f"  {score_column}: {int(scores_df[score_column].sum())}")
    exception_rows = mapping_df[mapping_df.get("采用的密码文件顺位", pd.Series(index=mapping_df.index, dtype=object)).isna()]
    print("输出文件:")
    for key, path in output_paths.items():
        print(f"  {key}: {path}")


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    round_count = len(args.password_files)
    validate_manual_order(args.password_order, round_count=round_count)
    validate_window_arguments(args, round_count=round_count)
    workers = resolve_worker_count(args.workers)

    name_file = Path(args.name_file)
    result_file = Path(args.result_file)
    password_files = [Path(path) for path in args.password_files]
    output_file = Path(args.output_file)
    window_config_file = Path(args.window_config_file) if args.window_config_file else None

    LOGGER.info(
        "开始执行签到匹配: name=%s result=%s output=%s round_count=%s",
        name_file,
        result_file,
        output_file,
        round_count,
    )

    roster_df = read_table(name_file)
    result_df = read_table(result_file)
    roster_columns = detect_roster_columns(roster_df)
    result_columns = detect_result_columns(result_df)
    result_df[result_columns["submitted_at"]] = pd.to_datetime(
        result_df[result_columns["submitted_at"]], errors="coerce"
    )
    if result_df[result_columns["submitted_at"]].isna().any():
        invalid_rows = result_df.index[result_df[result_columns["submitted_at"]].isna()].tolist()
        raise ValueError(f"提交时间存在无法解析的行: {[row + 2 for row in invalid_rows]}")

    year = infer_year(result_df[result_columns["submitted_at"]], explicit_year=args.year)
    round_windows, window_note = resolve_round_windows(
        window_mode=args.window_mode,
        submitted_at=result_df[result_columns["submitted_at"]],
        year=year,
        manual_specs=args.time_window,
        window_config_file=window_config_file,
        round_count=round_count,
    )
    round_frames, outside_df = build_submission_frames(
        result_df=result_df,
        result_columns=result_columns,
        round_windows=round_windows,
    )
    LOGGER.info(
        "各窗口提交数: %s",
        {round_window.label: len(round_frames[round_window.index]) for round_window in round_windows},
    )

    password_sets = load_password_sets(password_files)
    chosen_mapping, mapping_df = choose_password_mapping(
        round_windows=round_windows,
        round_frames=round_frames,
        password_sets=password_sets,
        result_password_column=result_columns["password"],
        result_time_column=result_columns["submitted_at"],
        max_distance=args.max_distance,
        manual_order=args.password_order,
    )
    password_sets_by_round = {
        round_index: password_sets[password_index]
        for round_index, password_index in enumerate(chosen_mapping, start=1)
    }

    round_rows_for_matching: dict[int, list[tuple[int, str]]] = {}
    for round_index, round_df in round_frames.items():
        round_rows_for_matching[round_index] = [
            (submission_id, normalize_password(row[result_columns["password"]]))
            for submission_id, row in round_df.iterrows()
        ]

    round_matches = run_parallel_matching(
        round_rows=round_rows_for_matching,
        password_sets_by_round=password_sets_by_round,
        max_distance=args.max_distance,
        workers=workers,
    )

    student_id_to_row, unique_name_to_row = build_roster_lookup(roster_df, roster_columns)

    details_records: list[dict[str, Any]] = []
    valid_student_ids_by_round: dict[int, set[str]] = {round_window.index: set() for round_window in round_windows}
    for round_window in round_windows:
        round_details, valid_student_ids = collect_round_details(
            round_window=round_window,
            round_df=round_frames[round_window.index],
            round_matches=round_matches[round_window.index],
            roster_df=roster_df,
            roster_columns=roster_columns,
            student_id_to_row=student_id_to_row,
            unique_name_to_row=unique_name_to_row,
            result_columns=result_columns,
            password_set=password_sets_by_round[round_window.index],
        )
        details_records.extend(round_details)
        valid_student_ids_by_round[round_window.index] = valid_student_ids

    details_records.extend(collect_outside_window_details(outside_df=outside_df, result_columns=result_columns))
    details_df = pd.DataFrame(details_records).sort_values(["提交时间", "结果表行号"], kind="stable")
    exceptions_df = build_exception_summary(details_df)
    scores_df = build_score_output(
        roster_df=roster_df,
        roster_columns=roster_columns,
        round_windows=round_windows,
        valid_student_ids_by_round=valid_student_ids_by_round,
    )

    output_paths = write_output(
        output_path=output_file,
        output_format=args.output_format,
        scores_df=scores_df,
        details_df=details_df,
        mapping_df=mapping_df,
        exceptions_df=exceptions_df,
    )
    print_summary(
        output_paths=output_paths,
        round_windows=round_windows,
        mapping_df=mapping_df,
        scores_df=scores_df,
        workers=workers,
        window_note=window_note,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"错误: {exc}", file=sys.stderr)
        raise SystemExit(1)