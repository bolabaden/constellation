#!/usr/bin/env python3
"""
Constellation CLI Entry Point

Allows running Constellation as a module: python -m constellation
"""

from __future__ import annotations

import asyncio
import sys

from constellation.core.service import main as constellation_main

if __name__ == "__main__":
    try:
        asyncio.run(constellation_main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e.__class__.__name__}: {e}")
        sys.exit(1)
