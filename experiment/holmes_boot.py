"""Container-only adapter for the pinned HolmesGPT SDK."""

import argparse
import json
from pathlib import Path

from holmes.config import Config
from holmes.core.tools import PrerequisiteCacheMode, ToolsetTag
from holmes.core.toolset_manager import load_builtin_toolsets
from holmes.core.tools_utils.filesystem_result_storage import tool_result_storage

LAB = Path("/lab")
CONFIG = Path("/tmp/experiment-config.yaml")
EXECUTOR_OPTIONS = {
    "toolset_tag_filter": [ToolsetTag.CORE, ToolsetTag.CLI],
    "enable_all_toolsets_possible": False,
    "prerequisite_cache": PrerequisiteCacheMode.FORCE_REFRESH,
}
EXPECTED_TOOLS = {
    "fetch_skill", "read_scaled_object", "read_hpa", "read_deployment",
    "read_kafka", "read_external_metric", "read_pods",
}


def load_config():
    settings = json.loads((LAB / "config.yaml").read_text())
    settings["toolsets"] = {
        toolset.name: {"enabled": toolset.name == "skills"}
        for toolset in load_builtin_toolsets(None, [str(LAB / "skills")])
    }
    CONFIG.write_text(json.dumps(settings))
    return Config.load_from_file(CONFIG, max_steps=12)


def verify_catalog(executor):
    tools = set(executor.tools_by_name)
    if tools != EXPECTED_TOOLS:
        raise SystemExit(f"Tool preflight failed: missing={EXPECTED_TOOLS-tools}, extra={tools-EXPECTED_TOOLS}")
    print("Verified snapshot catalog: " + ", ".join(sorted(tools)), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prompt", nargs="?")
    parser.add_argument("--model")
    parser.add_argument("--instructions")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    config = load_config()
    if args.preflight:
        verify_catalog(config.create_tool_executor(**EXECUTOR_OPTIONS))
        return
    if not all((args.prompt, args.model, args.instructions, args.output)):
        parser.error("prompt, model, instructions and output are required")
    with tool_result_storage() as store:
        agent = config.create_toolcalling_llm(model=args.model, tool_results_dir=store, **EXECUTOR_OPTIONS)
        # Check the same executor the model will actually use.
        verify_catalog(agent.tool_executor)
        result = agent.call(
            [
                {"role": "system", "content": args.instructions},
                {"role": "user", "content": args.prompt},
            ],
            response_format=json.loads((LAB / "response-format.json").read_text()),
        )
    args.output.write_text(result.model_dump_json(), encoding="utf8")


if __name__ == "__main__":
    main()
