#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, "/home/ubuntu/my-media-stack/src")

from constellation.core.container import ContainerConfig
from constellation.orchestration.export_manager import ExportConfig, ExportManager
from constellation.orchestration.import_manager import ImportManager

# Create the test compose content
original_compose: dict[str, Any] = {
    "version": "3.8",
    "services": {
        "app": {
            "image": "myapp:v1.0.0",
            "container_name": "my-app",
        }
    },
}

# Write to temp file
test_file: Path = Path("/tmp/test_compose.yml")
with test_file.open("w") as f:
    yaml.dump(original_compose, f)

# Import
import_manager: ImportManager = ImportManager()
containers: list[ContainerConfig] = import_manager.import_from_compose(test_file)

print("Imported container name:", containers[0].name)

# Export
export_config: ExportConfig = ExportConfig(output_dir=Path("/tmp"))
export_manager: ExportManager = ExportManager(export_config)
exported_file: Path = export_manager.export(containers, "compose")

# Check what was exported
with exported_file.open("r") as f:
    exported: dict[str, Any] = yaml.safe_load(f)

print("Exported services:", list(exported["services"].keys()))
print("Full exported content:")
print(yaml.dump(exported, default_flow_style=False))
