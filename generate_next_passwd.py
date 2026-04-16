#!/usr/bin/env python3

from __future__ import annotations

import sys

import password_generator


if __name__ == "__main__":
    raise SystemExit(password_generator.main(sys.argv[1:]))