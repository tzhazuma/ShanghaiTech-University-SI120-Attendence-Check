from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import password_generator as pg


def normalized_distance(left: str, right: str) -> int:
    left = pg.normalize_password(left)
    right = pg.normalize_password(right)
    return pg.limited_levenshtein(left, right, max_distance=max(len(left), len(right)))


class PasswordGeneratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.roster_df = pd.DataFrame(
            {
                "学号": [f"202600{i:04d}" for i in range(1, 21)],
                "姓名": [f"学生{i}" for i in range(1, 21)],
                "邮箱": [f"student{i}@example.com" for i in range(1, 21)],
            }
        )
        self.reference_passwords = [
            "Aa0Bb1Cc2Dd3",
            "Xy9Zw8Vu7Ts6",
            "Q1w2E3r4T5y6",
            "N7m8K9j0H1g2",
            "R2t4Y6u8I0o1",
        ]
        self.format_spec = pg.build_password_format(
            reference_passwords=self.reference_passwords,
            length=12,
            alphabet=pg.BASE62_ALPHABET,
            min_distance=6,
        )

    def create_reference_files(self, folder: Path, count: int = 4, suffix: str = ".xlsx") -> None:
        for index, password in enumerate(self.reference_passwords[:count], start=1):
            df = self.roster_df.iloc[:5].copy()
            df["password"] = [password, password[::-1], password[1:] + password[0], password, password[::-1]]
            if suffix == ".csv":
                df.to_csv(folder / f"passwd{index}.csv", index=False)
            else:
                df.to_excel(folder / f"passwd{index}.xlsx", index=False)

    def test_generation_is_deterministic_and_distance_safe(self) -> None:
        config = pg.GenerationConfig(
            algorithm="blake2-counter",
            seed_text="unit-test-seed",
            length=self.format_spec.length,
            alphabet=self.format_spec.alphabet,
            min_distance=self.format_spec.min_distance,
            workers=2,
            max_attempts_per_row=200,
        )

        passwords_one, metadata_one = pg.generate_passwords_for_roster(
            roster_df=self.roster_df,
            config=config,
            reference_passwords=self.reference_passwords,
        )
        passwords_two, metadata_two = pg.generate_passwords_for_roster(
            roster_df=self.roster_df,
            config=config,
            reference_passwords=self.reference_passwords,
        )

        self.assertEqual(passwords_one, passwords_two)
        self.assertEqual(metadata_one["generated_min_normalized_distance"], metadata_two["generated_min_normalized_distance"])
        self.assertEqual(len(passwords_one), len(self.roster_df))
        self.assertEqual(len({pg.normalize_password(password) for password in passwords_one}), len(passwords_one))

        for password in passwords_one:
            self.assertEqual(len(password), 12)
            for reference_password in self.reference_passwords:
                self.assertGreaterEqual(normalized_distance(password, reference_password), config.min_distance)

        for left_index, left in enumerate(passwords_one):
            for right in passwords_one[left_index + 1 :]:
                self.assertGreaterEqual(normalized_distance(left, right), config.min_distance)

    def test_cli_round_override_writes_default_issue_csv_and_metadata_examples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            roster_path = tmp_dir / "name.csv"
            self.roster_df.to_csv(roster_path, index=False)
            self.create_reference_files(tmp_dir, count=4, suffix=".csv")

            script_path = Path(__file__).resolve().parents[1] / "password_generator.py"
            subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--name-file",
                    str(roster_path),
                    "--round",
                    "9",
                    "--seed",
                    "cli-test-seed",
                    "--workers",
                    "1",
                ],
                check=True,
                cwd=tmp_dir,
            )

            output_path = tmp_dir / "passwd9.xlsx"
            issue_path = tmp_dir / "passwd9_issue.csv"
            metadata_path = tmp_dir / "passwd9_metadata.json"
            self.assertTrue(output_path.exists())
            self.assertTrue(issue_path.exists())
            self.assertTrue(metadata_path.exists())

            generated_df = pd.read_excel(output_path)
            self.assertEqual(list(generated_df.columns), ["学号", "姓名", "邮箱", "password"])
            issue_df = pd.read_csv(issue_path)
            self.assertEqual(list(issue_df.columns), ["学号", "password"])

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(metadata["seed"], "cli-test-seed")
            self.assertEqual(metadata["target_round"], 9)
            self.assertEqual(metadata["issue_format"], "csv")
            self.assertEqual(metadata["issue_file"], str(issue_path))
            self.assertTrue(metadata["auto_output_file_used"])
            self.assertGreater(len(metadata["closest_reference_examples"]), 0)
            self.assertGreater(len(metadata["closest_generated_examples"]), 0)

    def test_default_discovery_uses_name_and_next_passwd_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            self.roster_df.to_excel(tmp_dir / "name.xlsx", index=False)
            self.create_reference_files(tmp_dir, count=2, suffix=".xlsx")

            pd.DataFrame({"学号": ["1"], "姓名": ["测试"], "邮箱": ["a@example.com"]}).to_excel(
                tmp_dir / "passwd3.xlsx",
                index=False,
            )
            pd.DataFrame({"学号": ["1"], "password": ["Aa0Bb1Cc2Dd3"]}).to_excel(
                tmp_dir / "passwd2_issue.xlsx",
                index=False,
            )

            script_path = Path(__file__).resolve().parents[1] / "password_generator.py"
            subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--seed",
                    "default-discovery-seed",
                    "--workers",
                    "1",
                ],
                check=True,
                cwd=tmp_dir,
            )

            output_path = tmp_dir / "passwd4.xlsx"
            issue_path = tmp_dir / "passwd4_issue.csv"
            metadata_path = tmp_dir / "passwd4_metadata.json"
            self.assertTrue(output_path.exists())
            self.assertTrue(issue_path.exists())
            self.assertTrue(metadata_path.exists())

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertTrue(metadata["auto_name_file_used"])
            self.assertTrue(metadata["auto_output_file_used"])
            self.assertTrue(metadata["auto_reference_discovery_used"])
            self.assertTrue(metadata["auto_issue_file_used"])
            self.assertEqual(metadata["target_round"], 4)
            self.assertEqual(metadata["output_file"], str(output_path))
            self.assertEqual(metadata["issue_file"], str(issue_path))
            self.assertEqual(metadata["skipped_reference_files"][0]["path"], str(tmp_dir / "passwd3.xlsx"))

            generated_df = pd.read_excel(output_path)
            self.assertEqual(list(generated_df.columns), ["学号", "姓名", "邮箱", "password"])
            issue_df = pd.read_csv(issue_path)
            self.assertEqual(list(issue_df.columns), ["学号", "password"])

    def test_check_script_default_discovery_uses_latest_passwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            self.roster_df.to_excel(tmp_dir / "name.xlsx", index=False)
            self.create_reference_files(tmp_dir, count=3, suffix=".xlsx")

            script_path = Path(__file__).resolve().parents[1] / "check_passwords.py"
            subprocess.run(
                [
                    sys.executable,
                    str(script_path),
                    "--log-level",
                    "quiet",
                ],
                check=True,
                cwd=tmp_dir,
            )

            report_path = tmp_dir / "passwd3_check.xlsx"
            self.assertTrue(report_path.exists())

            summary_df = pd.read_excel(report_path, sheet_name="summary")
            metrics = {row["metric"]: row["value"] for _, row in summary_df.iterrows()}
            self.assertEqual(int(metrics["target_round"]), 3)
            self.assertEqual(str(metrics["target_file"]), str(tmp_dir / "passwd3.xlsx"))

            closest_reference_df = pd.read_excel(report_path, sheet_name="closest_reference")
            self.assertGreater(len(closest_reference_df), 0)


if __name__ == "__main__":
    unittest.main()