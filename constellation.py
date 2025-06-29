#!/usr/bin/env python3
"""
Constellation - Distributed Container Orchestration Service

Standalone script to run the Constellation service.
"""

from __future__ import annotations

import asyncio
import sys

from pathlib import Path

# Add src to path so we can import constellation
sys.path.insert(0, str(Path(__file__).parent / "src"))


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
