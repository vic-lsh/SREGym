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
generate_variant_stream_by_class = _vg_mod.generate_variant_stream_by_class
generate_variant_stream_grouped = _vg_mod.generate_variant_stream_grouped
filter_variant_ids_by_spec = _vg_mod.filter_variant_ids_by_spec
AdaptiveScheduler = _vg_mod.AdaptiveScheduler

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

    def test_derived_params_passed_to_factory(self):
        """derived_params should inject extra kwargs without affecting variant ID."""
        lookup = {"KEY_A": "value_a", "KEY_B": "value_b"}
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="env",
            dimensions=[
                VariantDimension("env_var", ["KEY_A", "KEY_B"]),
            ],
            derived_params=lambda p: {"env_var_value": lookup[p["env_var"]]},
        )
        variants = generate_variants(spec)
        # Variant IDs should NOT include the derived value
        assert "env__v_KEY_A" in variants
        assert "env__v_KEY_B" in variants
        # But the factory should pass derived params to the constructor
        a = variants["env__v_KEY_A"]()
        assert a.env_var == "KEY_A"
        assert a.env_var_value == "value_a"
        b = variants["env__v_KEY_B"]()
        assert b.env_var == "KEY_B"
        assert b.env_var_value == "value_b"

    def test_derived_params_none_by_default(self):
        """Without derived_params, factories get only dimension params."""
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="plain",
            dimensions=[VariantDimension("x", [1])],
        )
        variants = generate_variants(spec)
        instance = variants["plain__v_1"]()
        assert instance.x == 1
        assert not hasattr(instance, "extra")


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


# ---------------------------------------------------------------------------
# Tests: generate_variant_stream_by_class (round-robin per problem class)
# ---------------------------------------------------------------------------

class TestGenerateVariantStreamByClass:
    """Tests for the round-robin-by-class variant stream."""

    # Helper: build variant IDs for multiple classes
    @staticmethod
    def _make_ids(class_counts: dict[str, int]) -> list[str]:
        """Create variant IDs like 'classA__v_0', 'classA__v_1', etc."""
        ids = []
        for cls, n in class_counts.items():
            for i in range(n):
                ids.append(f"{cls}__v_{i}")
        return ids

    def test_round_robin_interleaving(self):
        """Each round should contain exactly one variant per class."""
        ids = self._make_ids({"alpha": 5, "beta": 5, "gamma": 5})
        num_classes = 3
        # Request exactly 2 full rounds
        stream = generate_variant_stream_by_class(ids, count=6, seed=42)
        assert len(stream) == 6

        for round_start in range(0, 6, num_classes):
            round_items = stream[round_start:round_start + num_classes]
            classes = [v.split("__v_")[0] for v in round_items]
            assert set(classes) == {"alpha", "beta", "gamma"}, (
                f"Round starting at {round_start} missing classes: {classes}"
            )

    def test_all_classes_in_every_full_round(self):
        """With uneven class sizes, every full round still has all classes."""
        ids = self._make_ids({"big": 20, "small": 2, "medium": 5})
        num_classes = 3
        num_rounds = 4
        stream = generate_variant_stream_by_class(
            ids, count=num_classes * num_rounds, seed=7
        )
        for r in range(num_rounds):
            start = r * num_classes
            round_items = stream[start:start + num_classes]
            classes = {v.split("__v_")[0] for v in round_items}
            assert classes == {"big", "small", "medium"}, (
                f"Round {r} classes: {classes}"
            )

    def test_small_class_wraps(self):
        """A class with fewer variants than rounds should repeat variants."""
        ids = self._make_ids({"only_one": 1, "many": 10})
        num_classes = 2
        stream = generate_variant_stream_by_class(ids, count=6, seed=42)
        only_one_picks = [v for v in stream if v.startswith("only_one__v_")]
        # Should appear 3 times (3 rounds), always the same variant
        assert len(only_one_picks) == 3
        assert all(v == "only_one__v_0" for v in only_one_picks)

    def test_deterministic(self):
        ids = self._make_ids({"a": 3, "b": 4, "c": 2})
        s1 = generate_variant_stream_by_class(ids, count=15, seed=42)
        s2 = generate_variant_stream_by_class(ids, count=15, seed=42)
        assert s1 == s2

    def test_different_seed_different_order(self):
        ids = self._make_ids({"a": 3, "b": 4, "c": 2})
        s1 = generate_variant_stream_by_class(ids, count=15, seed=42)
        s2 = generate_variant_stream_by_class(ids, count=15, seed=99)
        assert s1 != s2

    def test_offset_is_prefix_skip(self):
        ids = self._make_ids({"a": 5, "b": 5, "c": 5})
        full = generate_variant_stream_by_class(ids, count=12, offset=0, seed=42)
        tail = generate_variant_stream_by_class(ids, count=6, offset=6, seed=42)
        assert full[6:] == tail

    def test_offset_across_rounds(self):
        ids = self._make_ids({"x": 3, "y": 3})
        full = generate_variant_stream_by_class(ids, count=8, offset=0, seed=42)
        tail = generate_variant_stream_by_class(ids, count=4, offset=4, seed=42)
        assert full[4:] == tail

    def test_empty_ids_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            generate_variant_stream_by_class([], count=5)

    def test_zero_count_raises(self):
        with pytest.raises(ValueError, match="count must be > 0"):
            generate_variant_stream_by_class(["a__v_0"], count=0)

    def test_input_order_irrelevant(self):
        ids1 = ["b__v_1", "a__v_0", "b__v_0", "a__v_1"]
        ids2 = ["a__v_0", "a__v_1", "b__v_0", "b__v_1"]
        s1 = generate_variant_stream_by_class(ids1, count=8, seed=42)
        s2 = generate_variant_stream_by_class(ids2, count=8, seed=42)
        assert s1 == s2

    def test_single_class_degenerates_to_flat(self):
        """With one class, round-robin is just one variant per round."""
        ids = self._make_ids({"solo": 5})
        stream = generate_variant_stream_by_class(ids, count=5, seed=42)
        assert len(stream) == 5
        assert set(stream) == {f"solo__v_{i}" for i in range(5)}


# ---------------------------------------------------------------------------
# Tests: generate_variant_stream_grouped (drain one class before the next)
# ---------------------------------------------------------------------------

class TestGenerateVariantStreamGrouped:
    """Tests for the grouped-by-class variant stream."""

    @staticmethod
    def _make_ids(class_counts: dict[str, int]) -> list[str]:
        ids = []
        for cls, n in class_counts.items():
            for i in range(n):
                ids.append(f"{cls}__v_{i}")
        return ids

    @staticmethod
    def _classes_of(stream: list[str]) -> list[str]:
        return [v.split("__v_")[0] for v in stream]

    def test_drains_one_class_before_next(self):
        """With max_per_class=None, all of one class appears before another starts."""
        ids = self._make_ids({"alpha": 3, "beta": 4, "gamma": 2})
        # One full epoch = 3 + 4 + 2 = 9 entries
        stream = generate_variant_stream_grouped(ids, count=9, seed=42)
        assert len(stream) == 9
        classes = self._classes_of(stream)
        # Each class appears in a single contiguous block (3 distinct blocks total)
        block_starts = [
            i for i in range(len(classes)) if i == 0 or classes[i] != classes[i - 1]
        ]
        assert len(block_starts) == 3, f"expected 3 contiguous blocks, got: {classes}"
        # Every variant of each class is present in its block
        for cls, n in {"alpha": 3, "beta": 4, "gamma": 2}.items():
            block = [v for v in stream if v.startswith(f"{cls}__v_")]
            assert set(block) == {f"{cls}__v_{i}" for i in range(n)}

    def test_max_per_class_caps_block_size(self):
        """With max_per_class=2 and a 5-variant class, only 2 are emitted per epoch."""
        ids = self._make_ids({"big": 5, "other": 5})
        # epoch_size = min(5,2) + min(5,2) = 4
        stream = generate_variant_stream_grouped(
            ids, count=4, seed=42, max_per_class=2
        )
        big_in_epoch = [v for v in stream if v.startswith("big__v_")]
        other_in_epoch = [v for v in stream if v.startswith("other__v_")]
        assert len(big_in_epoch) == 2
        assert len(other_in_epoch) == 2
        # Within the epoch, the two classes appear in contiguous blocks.
        classes = self._classes_of(stream)
        boundary = [
            i for i in range(len(classes)) if i == 0 or classes[i] != classes[i - 1]
        ]
        assert len(boundary) == 2

    def test_max_per_class_larger_than_group(self):
        """If max_per_class > class size, only the available variants are emitted."""
        ids = self._make_ids({"tiny": 2, "huge": 10})
        # epoch_size = min(2,5) + min(10,5) = 2 + 5 = 7
        stream = generate_variant_stream_grouped(
            ids, count=7, seed=42, max_per_class=5
        )
        tiny_in_epoch = [v for v in stream if v.startswith("tiny__v_")]
        huge_in_epoch = [v for v in stream if v.startswith("huge__v_")]
        assert len(tiny_in_epoch) == 2
        assert set(tiny_in_epoch) == {"tiny__v_0", "tiny__v_1"}
        assert len(huge_in_epoch) == 5

    def test_class_order_shuffled_per_epoch(self):
        """Different epochs should generally yield different class orderings."""
        ids = self._make_ids({"a": 2, "b": 2, "c": 2})
        # epoch_size = 6, so two epochs = 12
        stream = generate_variant_stream_grouped(ids, count=12, seed=42)
        classes = self._classes_of(stream)
        epoch1_starts = [classes[0], classes[2], classes[4]]
        epoch2_starts = [classes[6], classes[8], classes[10]]
        # Class blocks should appear within each epoch (each set is the full set)
        assert set(epoch1_starts) == {"a", "b", "c"}
        assert set(epoch2_starts) == {"a", "b", "c"}

    def test_deterministic(self):
        ids = self._make_ids({"a": 3, "b": 4, "c": 2})
        s1 = generate_variant_stream_grouped(ids, count=15, seed=42)
        s2 = generate_variant_stream_grouped(ids, count=15, seed=42)
        assert s1 == s2

    def test_different_seed_different_order(self):
        ids = self._make_ids({"a": 3, "b": 4, "c": 2})
        s1 = generate_variant_stream_grouped(ids, count=15, seed=42)
        s2 = generate_variant_stream_grouped(ids, count=15, seed=99)
        assert s1 != s2

    def test_offset_is_prefix_skip(self):
        ids = self._make_ids({"a": 3, "b": 4, "c": 2})
        full = generate_variant_stream_grouped(ids, count=12, offset=0, seed=42)
        tail = generate_variant_stream_grouped(ids, count=6, offset=6, seed=42)
        assert full[6:] == tail

    def test_offset_across_epochs(self):
        ids = self._make_ids({"x": 2, "y": 2})
        # epoch_size = 4
        full = generate_variant_stream_grouped(ids, count=10, offset=0, seed=42)
        tail = generate_variant_stream_grouped(ids, count=5, offset=5, seed=42)
        assert full[5:] == tail

    def test_offset_with_max_per_class(self):
        ids = self._make_ids({"a": 5, "b": 5})
        full = generate_variant_stream_grouped(
            ids, count=12, offset=0, seed=42, max_per_class=3
        )
        tail = generate_variant_stream_grouped(
            ids, count=6, offset=6, seed=42, max_per_class=3
        )
        assert full[6:] == tail

    def test_input_order_irrelevant(self):
        ids1 = ["b__v_1", "a__v_0", "b__v_0", "a__v_1"]
        ids2 = ["a__v_0", "a__v_1", "b__v_0", "b__v_1"]
        s1 = generate_variant_stream_grouped(ids1, count=8, seed=42)
        s2 = generate_variant_stream_grouped(ids2, count=8, seed=42)
        assert s1 == s2

    def test_empty_ids_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            generate_variant_stream_grouped([], count=5)

    def test_zero_count_raises(self):
        with pytest.raises(ValueError, match="count must be > 0"):
            generate_variant_stream_grouped(["a__v_0"], count=0)

    def test_invalid_max_per_class_raises(self):
        with pytest.raises(ValueError, match="max_per_class must be > 0"):
            generate_variant_stream_grouped(
                ["a__v_0"], count=1, max_per_class=0
            )

    def test_single_class_grouped(self):
        """With one class, grouped is just a shuffled drain of that class."""
        ids = self._make_ids({"solo": 5})
        stream = generate_variant_stream_grouped(ids, count=5, seed=42)
        assert len(stream) == 5
        assert set(stream) == {f"solo__v_{i}" for i in range(5)}


# ---------------------------------------------------------------------------
# Tests: filter_variant_ids_by_spec
# ---------------------------------------------------------------------------


class TestFilterVariantIdsBySpec:
    POOL = [
        "readiness_probe_misconfiguration__v_social_network_user-service",
        "readiness_probe_misconfiguration__v_hotel_reservation_frontend",
        "missing_env_variable__v_astronomy_shop_frontend_CART_ADDR",
        "missing_env_variable__v_astronomy_shop_frontend_PRODUCT_CATALOG_ADDR",
        "wrong_dns_policy__v_social_network_user-service",
    ]
    KNOWN = {
        "readiness_probe_misconfiguration",
        "missing_env_variable",
        "wrong_dns_policy",
        "stale_coredns_config",
    }

    def test_single_spec_keeps_only_matching(self):
        result = filter_variant_ids_by_spec(
            self.POOL,
            ["readiness_probe_misconfiguration"],
            self.KNOWN,
        )
        assert result == [
            "readiness_probe_misconfiguration__v_social_network_user-service",
            "readiness_probe_misconfiguration__v_hotel_reservation_frontend",
        ]

    def test_multiple_specs_unioned(self):
        result = filter_variant_ids_by_spec(
            self.POOL,
            ["readiness_probe_misconfiguration", "missing_env_variable"],
            self.KNOWN,
        )
        assert set(result) == {
            "readiness_probe_misconfiguration__v_social_network_user-service",
            "readiness_probe_misconfiguration__v_hotel_reservation_frontend",
            "missing_env_variable__v_astronomy_shop_frontend_CART_ADDR",
            "missing_env_variable__v_astronomy_shop_frontend_PRODUCT_CATALOG_ADDR",
        }
        # Order preserved from input pool.
        assert result == [vid for vid in self.POOL if vid in set(result)]

    def test_unknown_spec_raises(self):
        with pytest.raises(ValueError, match="Unknown variant spec name"):
            filter_variant_ids_by_spec(
                self.POOL, ["totally_made_up_spec"], self.KNOWN,
            )

    def test_unknown_spec_message_lists_valid_names(self):
        with pytest.raises(ValueError) as exc_info:
            filter_variant_ids_by_spec(
                self.POOL, ["nope"], self.KNOWN,
            )
        # Sorted list of known names should appear in the error.
        for name in self.KNOWN:
            assert name in str(exc_info.value)

    def test_empty_spec_list_returns_empty(self):
        assert filter_variant_ids_by_spec(self.POOL, [], self.KNOWN) == []

    def test_spec_with_no_matching_ids_returns_empty(self):
        # 'stale_coredns_config' is a known name but no IDs in the pool match.
        assert filter_variant_ids_by_spec(
            self.POOL, ["stale_coredns_config"], self.KNOWN,
        ) == []

    def test_works_with_real_specs(self):
        """End-to-end: real spec names from variant_specs.py validate cleanly."""
        all_specs = get_all_variant_specs()
        all_variants = generate_all_variants(all_specs)
        all_ids = list(all_variants.keys())
        known = {spec.base_name for spec in all_specs}

        result = filter_variant_ids_by_spec(
            all_ids, ["readiness_probe_misconfiguration"], known,
        )
        assert len(result) > 0
        assert all(
            vid.startswith("readiness_probe_misconfiguration__v_") for vid in result
        )


# ---------------------------------------------------------------------------
# Tests: AdaptiveScheduler
# ---------------------------------------------------------------------------

def _drain(scheduler, max_steps: int = 1000) -> list[tuple[int, str]]:
    """Pull next_problem until None or step cap reached."""
    out = []
    for _ in range(max_steps):
        nxt = scheduler.next_problem()
        if nxt is None:
            return out
        out.append(nxt)
    raise AssertionError(f"scheduler did not terminate after {max_steps} steps")


class TestAdaptiveScheduler:
    POOL = [
        "class_a__v_1",
        "class_a__v_2",
        "class_a__v_3",
        "class_a__v_4",
        "class_b__v_1",
        "class_b__v_2",
        "class_c__v_1",
    ]

    def test_class_order_is_sorted(self):
        # Insert pool in scrambled order; class order should still be sorted.
        pool = ["zclass__v_a", "aclass__v_a", "mclass__v_a"]
        sched = AdaptiveScheduler(pool, consec_solves_to_stop=1, max_per_class=1)
        assert sched.class_order == ["aclass", "mclass", "zclass"]

    def test_seq_idx_is_monotonic(self):
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=10, max_per_class=3, seed=7
        )
        seqs = []
        for _ in range(5):
            nxt = sched.next_problem()
            assert nxt is not None
            seqs.append(nxt[0])
        assert seqs == [0, 1, 2, 3, 4]

    def test_seq_idx_starts_at_offset(self):
        sched = AdaptiveScheduler(
            self.POOL,
            consec_solves_to_stop=10,
            max_per_class=2,
            start_seq_idx=42,
        )
        first = sched.next_problem()
        assert first is not None
        assert first[0] == 42

    def test_mastery_short_circuits_class(self):
        """N consecutive solves drains current class and advances."""
        sched = AdaptiveScheduler(
            self.POOL,
            consec_solves_to_stop=3,
            max_per_class=10,
            seed=123,
        )
        # First three picks should all come from class_a (sorted order)
        # because we report them as solved before any other class is touched.
        picks = []
        for _ in range(3):
            seq, pid = sched.next_problem()
            picks.append(pid)
            sched.record_completion(pid, solved=True)
        assert all(p.startswith("class_a__") for p in picks)
        # Next pick must move to class_b — class_a is mastered.
        seq, pid = sched.next_problem()
        assert pid.startswith("class_b__")

    def test_max_per_class_advances_after_budget(self):
        """X failed attempts drain the class even without mastery."""
        sched = AdaptiveScheduler(
            self.POOL,
            consec_solves_to_stop=5,
            max_per_class=3,
            seed=11,
        )
        for _ in range(3):
            seq, pid = sched.next_problem()
            assert pid.startswith("class_a__")
            sched.record_completion(pid, solved=False)
        # Budget spent — next pick must be class_b.
        seq, pid = sched.next_problem()
        assert pid.startswith("class_b__")

    def test_pool_reshuffle_allows_repeats(self):
        """When the pool is smaller than max_per_class, repeats are allowed."""
        pool = ["only_class__v_a", "only_class__v_b"]  # pool size 2
        sched = AdaptiveScheduler(
            pool,
            consec_solves_to_stop=10,  # never reachable
            max_per_class=6,
            seed=42,
        )
        picks = []
        for _ in range(6):
            seq, pid = sched.next_problem()
            picks.append(pid)
            sched.record_completion(pid, solved=False)
        # Six picks emitted — both ids appear, with repeats.
        assert len(picks) == 6
        assert set(picks) == {"only_class__v_a", "only_class__v_b"}
        # After 6 attempts the class is done.
        assert sched.next_problem() is None

    def test_full_run_terminates(self):
        """Drain a fixed pool until None is returned."""
        sched = AdaptiveScheduler(
            self.POOL,
            consec_solves_to_stop=2,
            max_per_class=3,
            seed=99,
        )
        results = []
        while True:
            nxt = sched.next_problem()
            if nxt is None:
                break
            results.append(nxt)
            # Always fail so each class drains to max_per_class.
            sched.record_completion(nxt[1], solved=False)
        # Three classes * 3 attempts each = 9 picks.
        assert len(results) == 9
        seq_ids = [s for s, _ in results]
        assert seq_ids == list(range(9))

    def test_replay_rebuilds_state(self):
        """A fresh scheduler replayed with the same outcomes matches a live one."""
        live = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=2, max_per_class=4, seed=5,
        )
        # Run live for 6 picks with mixed outcomes.
        outcomes: list[tuple[str, bool]] = []
        for i in range(6):
            seq, pid = live.next_problem()
            solved = (i % 2 == 0)  # alternating
            live.record_completion(pid, solved=solved)
            outcomes.append((pid, solved))

        # Replay: feed the same outcomes to a new scheduler. We don't call
        # next_problem during replay; we rely on record_completion alone.
        replayed = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=2, max_per_class=4, seed=5,
        )
        for pid, solved in outcomes:
            replayed.record_completion(pid, solved=solved)
        replayed.set_next_seq_idx(len(outcomes))

        # Both schedulers should now produce identical next picks.
        for _ in range(8):
            a = live.next_problem()
            b = replayed.next_problem()
            assert a == b
            if a is None:
                break
            # Mirror further outcomes so the schedulers stay in lockstep.
            live.record_completion(a[1], solved=True)
            replayed.record_completion(b[1], solved=True)

    def test_record_completion_unknown_class_is_ignored(self):
        """Replay should be robust to ids from outside the current pool."""
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=1, max_per_class=1,
        )
        # Should not raise.
        sched.record_completion("not_a_real_class__v_x", solved=True)
        nxt = sched.next_problem()
        assert nxt is not None

    def test_class_state_summary_reflects_progress(self):
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=2, max_per_class=3, seed=1,
        )
        for _ in range(2):
            seq, pid = sched.next_problem()
            sched.record_completion(pid, solved=True)
        summary = sched.class_state_summary()
        assert summary["class_a"]["attempts"] == 2
        assert summary["class_a"]["recent_solves"] == [True, True]
        assert summary["class_a"]["done"] is True
        assert summary["class_b"]["attempts"] == 0
        assert summary["class_b"]["done"] is False

    def test_invalid_args_raise(self):
        with pytest.raises(ValueError):
            AdaptiveScheduler([], consec_solves_to_stop=1, max_per_class=1)
        with pytest.raises(ValueError):
            AdaptiveScheduler(self.POOL, consec_solves_to_stop=0, max_per_class=1)
        with pytest.raises(ValueError):
            AdaptiveScheduler(self.POOL, consec_solves_to_stop=1, max_per_class=0)
        with pytest.raises(ValueError):
            AdaptiveScheduler(self.POOL, consec_solves_to_stop=1, max_per_class=1, max_total=0)

    def test_max_total_stops_at_n(self):
        """max_total caps total dispatches before per-class budgets are spent."""
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=5, max_per_class=10, max_total=3,
        )
        dispatched = []
        while True:
            nxt = sched.next_problem()
            if nxt is None:
                break
            dispatched.append(nxt)
            sched.record_completion(nxt[1], solved=False)
        assert len(dispatched) == 3
        assert sched.is_done()

    def test_max_total_is_done_mid_class(self):
        """is_done() returns True as soon as max_total is reached."""
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=5, max_per_class=10, max_total=2,
        )
        assert not sched.is_done()
        sched.next_problem()
        assert not sched.is_done()
        sched.next_problem()
        # next_seq_idx is now 2 == max_total
        assert sched.is_done()
        assert sched.next_problem() is None

    def test_max_total_none_preserves_existing_behavior(self):
        """max_total=None leaves existing termination behavior unchanged."""
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=1, max_per_class=1, max_total=None,
        )
        dispatched = []
        while True:
            nxt = sched.next_problem()
            if nxt is None:
                break
            dispatched.append(nxt)
            sched.record_completion(nxt[1], solved=True)
        # One problem per class (3 classes), each mastered after 1 solve.
        assert len(dispatched) == 3
        assert sched.is_done()

    def test_max_total_respected_after_resume(self):
        """Replaying completions restores next_seq_idx; max_total still fires."""
        # Simulate a run that completed 2 problems, then resumed with max_total=3.
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=5, max_per_class=10, max_total=3,
        )
        p0 = sched.next_problem()
        p1 = sched.next_problem()
        assert p0 is not None and p1 is not None

        # Replay into a fresh scheduler (simulating resume).
        fresh = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=5, max_per_class=10, max_total=3,
        )
        fresh.record_completion(p0[1], solved=False)
        fresh.record_completion(p1[1], solved=False)
        fresh.set_next_seq_idx(2)

        # Only one more problem should be dispatched before hitting max_total=3.
        nxt = fresh.next_problem()
        assert nxt is not None
        assert nxt[0] == 2
        assert fresh.next_problem() is None
        assert fresh.is_done()

    def test_set_next_seq_idx_no_backwards(self):
        sched = AdaptiveScheduler(
            self.POOL, consec_solves_to_stop=1, max_per_class=1,
        )
        sched.next_problem()  # advances next_seq_idx to 1
        with pytest.raises(ValueError):
            sched.set_next_seq_idx(0)
        sched.set_next_seq_idx(5)
        nxt = sched.next_problem()
        assert nxt is not None
        assert nxt[0] == 5
