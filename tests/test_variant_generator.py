"""Tests for the problem variant generation system.

Uses importlib to load modules directly, bypassing sregym.conductor.__init__.py
which eagerly imports the full Conductor and all its heavy dependencies.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Direct-import helpers: load our modules without triggering the conductor
# import chain (which requires kubernetes, langchain, dotenv, etc.)
# ---------------------------------------------------------------------------

_PROBLEMS_DIR = Path(__file__).resolve().parent.parent / "sregym" / "conductor" / "problems"


def _load_module(name: str, filepath: Path):
    """Load a single Python module by file path, caching in sys.modules."""
    fqn = f"sregym.conductor.problems.{name}"
    if fqn in sys.modules:
        return sys.modules[fqn]
    spec = importlib.util.spec_from_file_location(fqn, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[fqn] = mod
    spec.loader.exec_module(mod)
    return mod


def _ensure_app_stubs():
    """Create lightweight stubs for app classes so variant_utils can import."""
    for mod_name in (
        "sregym.service.apps.astronomy_shop",
        "sregym.service.apps.hotel_reservation",
        "sregym.service.apps.social_network",
        "sregym.service.apps.train_ticket",
    ):
        if mod_name not in sys.modules:
            stub = type(sys)(mod_name)

            class _FakeApp:
                pass

            # Each stub module exposes the expected class name
            cls_name = mod_name.rsplit(".", 1)[-1]
            # CamelCase: astronomy_shop -> AstronomyShop
            camel = "".join(w.capitalize() for w in cls_name.split("_"))
            setattr(stub, camel, _FakeApp)
            sys.modules[mod_name] = stub


def _ensure_problem_class_stubs():
    """Create stubs for every problem class imported by variant_specs.py."""
    class _FakeProblemClass:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    mapping = {
        "duplicate_pvc_mounts": "DuplicatePVCMounts",
        "incorrect_port_assignment": "IncorrectPortAssignment",
        "liveness_probe_misconfiguration": "LivenessProbeMisconfiguration",
        "liveness_probe_too_aggressive": "LivenessProbeTooAggressive",
        "missing_configmap": "MissingConfigMap",
        "missing_env_variable": "MissingEnvVariable",
        "missing_service": "MissingService",
        "readiness_probe_misconfiguration": "ReadinessProbeMisconfiguration",
        "rolling_update_misconfigured": "RollingUpdateMisconfigured",
        "service_dns_resolution_failure": "ServiceDNSResolutionFailure",
        "service_port_conflict": "ServicePortConflict",
        "sidecar_port_conflict": "SidecarPortConflict",
        "stale_coredns_config": "StaleCoreDNSConfig",
        "target_port": "K8STargetPortMisconfig",
        "wrong_dns_policy": "WrongDNSPolicy",
        "wrong_service_selector": "WrongServiceSelector",
    }
    for mod_name, cls_name in mapping.items():
        fqn = f"sregym.conductor.problems.{mod_name}"
        if fqn not in sys.modules:
            stub = type(sys)(fqn)
            setattr(stub, cls_name, type(cls_name, (_FakeProblemClass,), {}))
            sys.modules[fqn] = stub


# Load our modules
_vg_mod = _load_module("variant_generator", _PROBLEMS_DIR / "variant_generator.py")
VariantDimension = _vg_mod.VariantDimension
VariantSpec = _vg_mod.VariantSpec
generate_variants = _vg_mod.generate_variants
generate_all_variants = _vg_mod.generate_all_variants
generate_variant_stream = _vg_mod.generate_variant_stream

_ensure_app_stubs()
_vu_mod = _load_module("variant_utils", _PROBLEMS_DIR / "variant_utils.py")
SERVICES_BY_APP = _vu_mod.SERVICES_BY_APP

_ensure_problem_class_stubs()
_vs_mod = _load_module("variant_specs", _PROBLEMS_DIR / "variant_specs.py")
get_all_variant_specs = _vs_mod.get_all_variant_specs


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

class FakeProblem:
    """Minimal problem-like class for testing."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# Tests: generate_variants core logic
# ---------------------------------------------------------------------------

class TestGenerateVariants:
    def test_single_dimension(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="test_problem",
            dimensions=[
                VariantDimension("app_name", ["social_network", "hotel_reservation"]),
            ],
        )
        variants = generate_variants(spec)
        assert len(variants) == 2
        assert "test_problem__v_social_network" in variants
        assert "test_problem__v_hotel_reservation" in variants

    def test_two_dimensions_cartesian_product(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="probe",
            dimensions=[
                VariantDimension("app", ["a1", "a2"]),
                VariantDimension("svc", ["s1", "s2", "s3"]),
            ],
        )
        variants = generate_variants(spec)
        assert len(variants) == 6  # 2 x 3
        assert "probe__v_a1_s1" in variants
        assert "probe__v_a2_s3" in variants

    def test_constraints_filter(self):
        valid_combos = {("a1", "s1"), ("a1", "s2"), ("a2", "s2")}
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="filtered",
            dimensions=[
                VariantDimension("app", ["a1", "a2"]),
                VariantDimension("svc", ["s1", "s2"]),
            ],
            constraints=lambda p: (p["app"], p["svc"]) in valid_combos,
        )
        variants = generate_variants(spec)
        assert len(variants) == 3
        assert "filtered__v_a1_s1" in variants
        assert "filtered__v_a1_s2" in variants
        assert "filtered__v_a2_s2" in variants
        assert "filtered__v_a2_s1" not in variants

    def test_empty_dimensions(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="empty",
            dimensions=[],
        )
        assert generate_variants(spec) == {}

    def test_all_filtered_out(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="none",
            dimensions=[VariantDimension("x", [1, 2, 3])],
            constraints=lambda p: False,
        )
        assert generate_variants(spec) == {}

    def test_factory_produces_correct_params(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="param_check",
            dimensions=[
                VariantDimension("app_name", ["astro"]),
                VariantDimension("port", [8080, 9090]),
            ],
        )
        variants = generate_variants(spec)
        instance = variants["param_check__v_astro_8080"]()
        assert instance.app_name == "astro"
        assert instance.port == 8080

        instance2 = variants["param_check__v_astro_9090"]()
        assert instance2.port == 9090

    def test_three_dimensions(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="multi",
            dimensions=[
                VariantDimension("a", [1, 2]),
                VariantDimension("b", ["x", "y"]),
                VariantDimension("c", [True]),
            ],
        )
        variants = generate_variants(spec)
        assert len(variants) == 4  # 2 x 2 x 1
        assert "multi__v_1_x_True" in variants
        assert "multi__v_2_y_True" in variants

    def test_id_format(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="my_problem",
            dimensions=[
                VariantDimension("app", ["social_network"]),
                VariantDimension("svc", ["user-service"]),
            ],
        )
        variants = generate_variants(spec)
        keys = list(variants.keys())
        assert len(keys) == 1
        assert keys[0] == "my_problem__v_social_network_user-service"

    def test_no_collision_within_spec(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="unique",
            dimensions=[
                VariantDimension("a", [1, 2, 3, 4, 5]),
                VariantDimension("b", ["x", "y", "z"]),
            ],
        )
        variants = generate_variants(spec)
        assert len(variants) == 15
        ids = list(variants.keys())
        assert len(ids) == len(set(ids))

    def test_closure_captures_correctly(self):
        """Verify each lambda captures its own params (not shared reference)."""
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="closure",
            dimensions=[VariantDimension("val", [10, 20, 30])],
        )
        variants = generate_variants(spec)
        results = {vid: factory().val for vid, factory in variants.items()}
        assert results["closure__v_10"] == 10
        assert results["closure__v_20"] == 20
        assert results["closure__v_30"] == 30


# ---------------------------------------------------------------------------
# Tests: generate_all_variants (multi-spec)
# ---------------------------------------------------------------------------

class TestGenerateAllVariants:
    def test_multiple_specs_combined(self):
        spec1 = VariantSpec(
            problem_class=FakeProblem,
            base_name="problem_a",
            dimensions=[VariantDimension("x", [1, 2])],
        )
        spec2 = VariantSpec(
            problem_class=FakeProblem,
            base_name="problem_b",
            dimensions=[VariantDimension("y", ["a", "b", "c"])],
        )
        all_variants = generate_all_variants([spec1, spec2])
        assert len(all_variants) == 5  # 2 + 3

    def test_collision_across_specs_raises(self):
        spec1 = VariantSpec(
            problem_class=FakeProblem,
            base_name="same",
            dimensions=[VariantDimension("x", [1])],
        )
        spec2 = VariantSpec(
            problem_class=FakeProblem,
            base_name="same",
            dimensions=[VariantDimension("x", [1])],
        )
        with pytest.raises(ValueError, match="collision"):
            generate_all_variants([spec1, spec2])

    def test_no_collision_different_base_names(self):
        spec1 = VariantSpec(
            problem_class=FakeProblem,
            base_name="alpha",
            dimensions=[VariantDimension("x", [1, 2])],
        )
        spec2 = VariantSpec(
            problem_class=FakeProblem,
            base_name="beta",
            dimensions=[VariantDimension("x", [1, 2])],
        )
        all_variants = generate_all_variants([spec1, spec2])
        assert len(all_variants) == 4
        assert "alpha__v_1" in all_variants
        assert "beta__v_1" in all_variants

    def test_empty_specs_list(self):
        assert generate_all_variants([]) == {}


# ---------------------------------------------------------------------------
# Tests: actual variant specs integration
# ---------------------------------------------------------------------------

class TestVariantSpecs:
    def test_specs_load_without_error(self):
        specs = get_all_variant_specs()
        assert len(specs) > 0

    def test_all_specs_generate_without_collision(self):
        specs = get_all_variant_specs()
        all_variants = generate_all_variants(specs)
        assert len(all_variants) > 100  # We expect 300+ variants

    def test_generated_ids_use_v_separator(self):
        specs = get_all_variant_specs()
        all_variants = generate_all_variants(specs)
        for vid in all_variants:
            assert "__v_" in vid, f"Variant ID '{vid}' missing __v_ separator"

    def test_no_empty_specs(self):
        specs = get_all_variant_specs()
        for spec in specs:
            variants = generate_variants(spec)
            assert len(variants) > 0, f"Spec '{spec.base_name}' produced zero variants"

    def test_services_by_app_coverage(self):
        assert "social_network" in SERVICES_BY_APP
        assert "hotel_reservation" in SERVICES_BY_APP
        assert "astronomy_shop" in SERVICES_BY_APP
        for app, services in SERVICES_BY_APP.items():
            assert len(services) > 0, f"No services for {app}"

    def test_variant_count_breakdown(self):
        """Verify each spec produces a reasonable number of variants."""
        specs = get_all_variant_specs()
        total = 0
        for spec in specs:
            variants = generate_variants(spec)
            count = len(variants)
            total += count
            # Every spec should produce at least 1 variant
            assert count >= 1, f"{spec.base_name} produced {count} variants"
        # Total should be substantial
        assert total >= 300, f"Only {total} total variants (expected 300+)"

    def test_no_duplicate_base_names(self):
        """Each spec should have a unique base_name."""
        specs = get_all_variant_specs()
        names = [s.base_name for s in specs]
        assert len(names) == len(set(names)), f"Duplicate base names: {names}"

    def test_factories_are_callable(self):
        """Every generated factory should be callable and produce an object."""
        specs = get_all_variant_specs()
        all_variants = generate_all_variants(specs)
        # Spot-check a sample of variants
        sample = list(all_variants.items())[:20]
        for vid, factory in sample:
            instance = factory()
            assert instance is not None, f"Factory for '{vid}' returned None"


# ---------------------------------------------------------------------------
# Tests: generate_variant_stream (epoch-based cycling)
# ---------------------------------------------------------------------------

class TestGenerateVariantStream:
    def test_deterministic(self):
        ids = ["a", "b", "c", "d", "e"]
        s1 = generate_variant_stream(ids, count=10, seed=42)
        s2 = generate_variant_stream(ids, count=10, seed=42)
        assert s1 == s2

    def test_different_seed_different_order(self):
        ids = ["a", "b", "c", "d", "e"]
        s1 = generate_variant_stream(ids, count=10, seed=42)
        s2 = generate_variant_stream(ids, count=10, seed=99)
        assert s1 != s2

    def test_offset_is_prefix_skip(self):
        ids = ["a", "b", "c", "d", "e"]
        full = generate_variant_stream(ids, count=10, offset=0, seed=42)
        tail = generate_variant_stream(ids, count=5, offset=5, seed=42)
        assert full[5:] == tail

    def test_epoch_covers_all(self):
        ids = ["a", "b", "c"]
        stream = generate_variant_stream(ids, count=6, seed=42)
        # First epoch has all 3, second epoch has all 3
        assert set(stream[:3]) == {"a", "b", "c"}
        assert set(stream[3:6]) == {"a", "b", "c"}

    def test_count_larger_than_pool(self):
        ids = ["x", "y"]
        stream = generate_variant_stream(ids, count=7, seed=42)
        assert len(stream) == 7

    def test_empty_ids_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            generate_variant_stream([], count=5)

    def test_zero_count_raises(self):
        with pytest.raises(ValueError, match="count must be > 0"):
            generate_variant_stream(["a"], count=0)

    def test_offset_mid_epoch(self):
        """Offset that lands in the middle of an epoch."""
        ids = ["a", "b", "c", "d"]
        full_epoch0 = generate_variant_stream(ids, count=4, offset=0, seed=7)
        partial = generate_variant_stream(ids, count=2, offset=2, seed=7)
        assert partial == full_epoch0[2:]

    def test_offset_across_epochs(self):
        """Offset that spans multiple epochs."""
        ids = ["a", "b", "c"]
        # Get 9 items (3 epochs) starting from 0
        full = generate_variant_stream(ids, count=9, offset=0, seed=42)
        # Get 3 items starting from offset 6 (epoch 2)
        tail = generate_variant_stream(ids, count=3, offset=6, seed=42)
        assert full[6:] == tail

    def test_input_order_irrelevant(self):
        """Canonical sort means input order doesn't matter."""
        s1 = generate_variant_stream(["c", "a", "b"], count=5, seed=42)
        s2 = generate_variant_stream(["b", "c", "a"], count=5, seed=42)
        assert s1 == s2
