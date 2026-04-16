#!/usr/bin/env python3

from __future__ import annotations

import sys

import check_passwords


if __name__ == "__main__":
    raise SystemExit(check_passwords.main(sys.argv[1:]))