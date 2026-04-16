#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from password_generator import (
    build_nearest_generated_examples,
    build_nearest_reference_examples,
    collect_reference_password_records,
    detect_password_column,
    discover_passwd_related_files,
    extract_passwd_index,
    find_column,
    inspect_password_file,
    normalize_password,
    normalize_student_id,
    read_table,
    resolve_name_file,
    resolve_output_mode,
    resolve_user_path,
    resolve_reference_files,
    write_table,
)


LOG_LEVELS = {
    "quiet": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}
LOGGER = logging.getLogger("check_passwords")


def find_optional_column(columns: Sequence[Any], candidate_groups: Sequence[Sequence[str]]) -> Any | None:
    try:
        return find_column(columns, candidate_groups, "optional")
    except ValueError:
        return None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "检查目标密码表与名单、历史参考密码之间的一致性和安全距离。"
            "不传参数时，会自动检查当前目录中最新的合法 passwdN 文件。"
        )
    )
    parser.add_argument("--search-dir", default=".", help="默认搜索目录，默认当前目录。")
    parser.add_argument("--name-file", help="名单文件；不传时自动发现。")
    parser.add_argument("--target-file", help="待检查的密码表；不传时自动使用最新的合法 passwdN 文件。")
    parser.add_argument(
        "--reference-files",
        nargs="*",
        default=None,
        help="参考密码表列表；不传时自动使用除 target 外的其他合法 passwdN 文件。",
    )
    parser.add_argument("--output-file", help="检查报告输出路径；不传时默认写成 <target_stem>_check.xlsx。")
    parser.add_argument(
        "--output-format",
        choices=("auto", "xlsx", "csv"),
        default="auto",
        help="检查报告格式，默认 auto。",
    )
    parser.add_argument("--max-examples", type=int, default=10, help="报告中保留多少条最接近样例，默认 10。")
    parser.add_argument(
        "--log-level",
        choices=("quiet", "info", "debug"),
        default="info",
        help="日志级别。",
    )
    return parser.parse_args(argv)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(level=LOG_LEVELS[log_level], format="[%(levelname)s] %(message)s")


def detect_target_columns(df: pd.DataFrame) -> dict[str, Any]:
    return {
        "student_id": find_column(df.columns, [["学号"], ["student", "id"]], "学号列"),
        "password": detect_password_column(df),
        "name": find_optional_column(df.columns, [["姓名"], ["名字"], ["name"]]),
        "email": find_optional_column(df.columns, [["邮箱"], ["email"]]),
    }


def discover_valid_passwd_files(search_dir: Path) -> list[Path]:
    valid_files: list[Path] = []
    for candidate in discover_passwd_related_files(search_dir):
        validation = inspect_password_file(candidate)
        if validation.index is None:
            continue
        if not validation.valid:
            LOGGER.warning("跳过不合法的密码文件 %s: %s", candidate, validation.reason)
            continue
        valid_files.append(candidate)
    return sorted(valid_files, key=lambda path: extract_passwd_index(path) or 0)


def resolve_target_file(explicit_target_file: str | None, search_dir: Path) -> tuple[Path, bool]:
    explicit_path = resolve_user_path(explicit_target_file, search_dir)
    if explicit_path is not None:
        validation = inspect_password_file(explicit_path)
        if not validation.valid:
            raise ValueError(f"待检查密码表不合法: {explicit_path}，原因: {validation.reason}")
        return explicit_path, False

    valid_files = discover_valid_passwd_files(search_dir)
    if not valid_files:
        raise ValueError(f"在 {search_dir} 中找不到可检查的合法 passwdN 文件")
    target_file = valid_files[-1]
    LOGGER.info("未显式提供 target-file，自动使用最新密码表: %s", target_file)
    return target_file, True


def resolve_check_reference_files(
    explicit_files: Sequence[str] | None,
    search_dir: Path,
    target_file: Path,
) -> tuple[list[Path], bool]:
    if explicit_files is not None:
        reference_files, _, _ = resolve_reference_files(explicit_files, search_dir)
        return [path for path in reference_files if path.resolve() != target_file.resolve()], False

    reference_files = [
        path
        for path in discover_valid_passwd_files(search_dir)
        if path.resolve() != target_file.resolve()
    ]
    LOGGER.info("未显式提供 reference-files，自动使用 %s 个参考密码表", len(reference_files))
    return reference_files, True


def resolve_output_path(
    output_file_text: str | None,
    target_file: Path,
    search_dir: Path,
    output_format: str,
) -> tuple[Path, str, bool]:
    explicit_path = resolve_user_path(output_file_text, search_dir)
    resolved_mode = resolve_output_mode(explicit_path, output_format)
    if explicit_path is None:
        suffix = ".csv" if resolved_mode == "csv" else ".xlsx"
        path = target_file.with_name(f"{target_file.stem}_check{suffix}")
        LOGGER.info("未显式提供 output-file，自动使用: %s", path)
        return path, resolved_mode, True
    if resolved_mode == "csv":
        return (explicit_path if explicit_path.suffix.lower() == ".csv" else explicit_path.with_suffix(".csv")), "csv", False
    return (explicit_path if explicit_path.suffix.lower() == ".xlsx" else explicit_path.with_suffix(".xlsx")), "xlsx", False


def build_target_profile(df: pd.DataFrame, columns: dict[str, Any]) -> pd.DataFrame:
    profile = pd.DataFrame(
        {
            "row_number": range(2, len(df) + 2),
            "student_id": df[columns["student_id"]].map(normalize_student_id),
            "password": df[columns["password"]].map(lambda value: "" if pd.isna(value) else str(value)),
        }
    )
    profile["normalized_password"] = profile["password"].map(normalize_password)
    profile["password_length"] = profile["password"].map(len)
    if columns["name"] is not None:
        profile["name"] = df[columns["name"]].fillna("").map(str)
    else:
        profile["name"] = ""
    return profile


def build_student_issue_frame(roster_df: pd.DataFrame, roster_columns: dict[str, Any], profile: pd.DataFrame) -> pd.DataFrame:
    roster_view = pd.DataFrame(
        {
            "student_id": roster_df[roster_columns["student_id"]].map(normalize_student_id),
            "name": roster_df[roster_columns["name"]].fillna("").map(str),
        }
    )
    roster_by_id = roster_view.drop_duplicates(subset=["student_id"]).set_index("student_id")
    target_ids = set(profile["student_id"].tolist())
    roster_ids = set(roster_view["student_id"].tolist())

    issues: list[dict[str, Any]] = []
    for student_id in sorted(roster_ids - target_ids):
        issues.append(
            {
                "issue_type": "missing_in_target",
                "student_id": student_id,
                "name": roster_by_id.loc[student_id, "name"] if student_id in roster_by_id.index else "",
                "count": 1,
            }
        )
    for student_id in sorted(target_ids - roster_ids):
        issues.append(
            {
                "issue_type": "extra_in_target",
                "student_id": student_id,
                "name": "",
                "count": 1,
            }
        )

    student_counts = Counter(profile["student_id"].tolist())
    for student_id, count in sorted(student_counts.items()):
        if student_id and count > 1:
            issues.append(
                {
                    "issue_type": "duplicate_student_id",
                    "student_id": student_id,
                    "name": roster_by_id.loc[student_id, "name"] if student_id in roster_by_id.index else "",
                    "count": count,
                }
            )

    return pd.DataFrame(issues, columns=["issue_type", "student_id", "name", "count"])


def build_password_duplicate_frame(profile: pd.DataFrame) -> pd.DataFrame:
    issues: list[dict[str, Any]] = []

    def append_duplicates(issue_type: str, key_column: str) -> None:
        grouped = profile.groupby(key_column, dropna=False)
        for key, group in grouped:
            key_text = "" if pd.isna(key) else str(key)
            if not key_text or len(group) <= 1:
                continue
            issues.append(
                {
                    "issue_type": issue_type,
                    "key": key_text,
                    "count": len(group),
                    "row_numbers": ",".join(str(value) for value in group["row_number"].tolist()),
                    "student_ids": ",".join(group["student_id"].tolist()),
                    "passwords": " | ".join(group["password"].tolist()),
                }
            )

    append_duplicates("raw_password_duplicate", "password")
    append_duplicates("normalized_password_duplicate", "normalized_password")
    return pd.DataFrame(issues, columns=["issue_type", "key", "count", "row_numbers", "student_ids", "passwords"])


def build_summary_frame(
    *,
    target_file: Path,
    auto_target_file_used: bool,
    output_path: Path,
    auto_output_file_used: bool,
    auto_reference_files_used: bool,
    auto_name_file_used: bool,
    target_round: int | None,
    roster_df: pd.DataFrame,
    profile: pd.DataFrame,
    reference_files: Sequence[Path],
    reference_password_records_count: int,
    student_issues: pd.DataFrame,
    password_duplicates: pd.DataFrame,
    closest_reference_examples: Sequence[dict[str, Any]],
    closest_internal_examples: Sequence[dict[str, Any]],
) -> pd.DataFrame:
    raw_duplicate_count = int((password_duplicates["issue_type"] == "raw_password_duplicate").sum()) if not password_duplicates.empty else 0
    normalized_duplicate_count = int((password_duplicates["issue_type"] == "normalized_password_duplicate").sum()) if not password_duplicates.empty else 0
    missing_student_count = int((student_issues["issue_type"] == "missing_in_target").sum()) if not student_issues.empty else 0
    extra_student_count = int((student_issues["issue_type"] == "extra_in_target").sum()) if not student_issues.empty else 0
    duplicate_student_id_count = int((student_issues["issue_type"] == "duplicate_student_id").sum()) if not student_issues.empty else 0
    exact_reference_collision_count = sum(1 for item in closest_reference_examples if item["normalized_distance"] == 0)
    length_distribution = dict(sorted(Counter(profile["password_length"].tolist()).items()))

    summary_rows = [
        {"metric": "target_file", "value": str(target_file)},
        {"metric": "target_round", "value": "" if target_round is None else target_round},
        {"metric": "target_row_count", "value": len(profile)},
        {"metric": "roster_row_count", "value": len(roster_df)},
        {"metric": "reference_file_count", "value": len(reference_files)},
        {"metric": "reference_password_count", "value": reference_password_records_count},
        {"metric": "auto_target_file_used", "value": auto_target_file_used},
        {"metric": "auto_reference_files_used", "value": auto_reference_files_used},
        {"metric": "auto_name_file_used", "value": auto_name_file_used},
        {"metric": "auto_output_file_used", "value": auto_output_file_used},
        {"metric": "missing_student_count", "value": missing_student_count},
        {"metric": "extra_student_count", "value": extra_student_count},
        {"metric": "duplicate_student_id_count", "value": duplicate_student_id_count},
        {"metric": "raw_duplicate_password_count", "value": raw_duplicate_count},
        {"metric": "normalized_duplicate_password_count", "value": normalized_duplicate_count},
        {"metric": "closest_reference_distance", "value": "" if not closest_reference_examples else closest_reference_examples[0]["normalized_distance"]},
        {"metric": "closest_generated_distance", "value": "" if not closest_internal_examples else closest_internal_examples[0]["normalized_distance"]},
        {"metric": "exact_reference_collision_count", "value": exact_reference_collision_count},
        {"metric": "password_length_distribution", "value": json.dumps(length_distribution, ensure_ascii=False)},
        {"metric": "report_file", "value": str(output_path)},
    ]
    return pd.DataFrame(summary_rows)


def write_report(output_path: Path, output_mode: str, frames: dict[str, pd.DataFrame]) -> list[Path]:
    written_paths: list[Path] = []
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_mode == "csv":
        base_name = output_path.stem
        for key, frame in frames.items():
            path = output_path if key == "summary" else output_path.with_name(f"{base_name}_{key}.csv")
            write_table(frame, path)
            written_paths.append(path)
        return written_paths

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for key, frame in frames.items():
            sheet_name = key[:31]
            frame.to_excel(writer, sheet_name=sheet_name, index=False)
    written_paths.append(output_path)
    return written_paths


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    search_dir = Path(args.search_dir).resolve()
    if args.max_examples <= 0:
        raise ValueError("--max-examples 必须大于 0")

    name_file, auto_name_file_used = resolve_name_file(args.name_file, search_dir)
    roster_df = read_table(name_file)
    roster_columns = {
        "student_id": find_column(roster_df.columns, [["学号"], ["student", "id"]], "名单学号列"),
        "name": find_column(roster_df.columns, [["姓名"], ["名字"], ["name"]], "名单姓名列"),
    }

    target_file, auto_target_file_used = resolve_target_file(args.target_file, search_dir)
    target_round = extract_passwd_index(target_file)
    reference_files, auto_reference_files_used = resolve_check_reference_files(args.reference_files, search_dir, target_file)
    output_path, output_mode, auto_output_file_used = resolve_output_path(
        args.output_file,
        target_file,
        search_dir,
        args.output_format,
    )

    target_df = read_table(target_file)
    target_columns = detect_target_columns(target_df)
    profile = build_target_profile(target_df, target_columns)
    student_issues = build_student_issue_frame(roster_df, roster_columns, profile)
    password_duplicates = build_password_duplicate_frame(profile)
    reference_records = collect_reference_password_records(reference_files)
    closest_reference_examples = build_nearest_reference_examples(profile["password"].tolist(), reference_records, limit=len(profile)).copy()
    closest_internal_examples = build_nearest_generated_examples(profile["password"].tolist(), limit=max(args.max_examples, len(profile))).copy()

    summary_frame = build_summary_frame(
        target_file=target_file,
        auto_target_file_used=auto_target_file_used,
        output_path=output_path,
        auto_output_file_used=auto_output_file_used,
        auto_reference_files_used=auto_reference_files_used,
        auto_name_file_used=auto_name_file_used,
        target_round=target_round,
        roster_df=roster_df,
        profile=profile,
        reference_files=reference_files,
        reference_password_records_count=len(reference_records),
        student_issues=student_issues,
        password_duplicates=password_duplicates,
        closest_reference_examples=closest_reference_examples,
        closest_internal_examples=closest_internal_examples,
    )

    frames = {
        "summary": summary_frame,
        "student_issues": student_issues,
        "password_duplicates": password_duplicates,
        "closest_reference": pd.DataFrame(closest_reference_examples[: args.max_examples]),
        "closest_internal": pd.DataFrame(closest_internal_examples[: args.max_examples]),
        "target_profile": profile,
    }
    written_paths = write_report(output_path, output_mode, frames)

    print(f"待检查密码表: {target_file}")
    print(f"参考密码表数量: {len(reference_files)}")
    print(f"检查报告: {written_paths[0]}")
    if output_mode == "csv" and len(written_paths) > 1:
        print(f"附加 CSV 文件数量: {len(written_paths) - 1}")
    print(f"最近参考距离: {'' if not closest_reference_examples else closest_reference_examples[0]['normalized_distance']}")
    print(f"最近内部距离: {'' if not closest_internal_examples else closest_internal_examples[0]['normalized_distance']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"错误: {exc}", file=sys.stderr)
        raise SystemExit(1)