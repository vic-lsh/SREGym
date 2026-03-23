"""Tests for the problem variant generation system."""

import pytest

from sregym.conductor.problems.variant_generator import (
    VariantDimension,
    VariantSpec,
    generate_all_variants,
    generate_variants,
)
from sregym.conductor.problems.variant_utils import SERVICES_BY_APP


# --- Fixtures / Helpers ---

class FakeProblem:
    """Minimal problem-like class for testing."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# --- generate_variants tests ---

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
        assert len(variants) == 6  # 2 × 3
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
        variants = generate_variants(spec)
        assert variants == {}

    def test_all_filtered_out(self):
        spec = VariantSpec(
            problem_class=FakeProblem,
            base_name="none",
            dimensions=[
                VariantDimension("x", [1, 2, 3]),
            ],
            constraints=lambda p: False,
        )
        variants = generate_variants(spec)
        assert variants == {}

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
        assert len(variants) == 4  # 2 × 2 × 1
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
        """All generated IDs within a single spec must be unique."""
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
        # All keys unique by construction (dict), but let's verify values are distinct callables
        ids = list(variants.keys())
        assert len(ids) == len(set(ids))


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


class TestVariantSpecs:
    """Test the actual variant specs from variant_specs.py."""

    def test_specs_load_without_error(self):
        from sregym.conductor.problems.variant_specs import get_all_variant_specs
        specs = get_all_variant_specs()
        assert len(specs) > 0

    def test_all_specs_generate_without_collision(self):
        from sregym.conductor.problems.variant_specs import get_all_variant_specs
        specs = get_all_variant_specs()
        all_variants = generate_all_variants(specs)
        assert len(all_variants) > 100  # We expect 400+ variants

    def test_generated_ids_use_v_separator(self):
        from sregym.conductor.problems.variant_specs import get_all_variant_specs
        specs = get_all_variant_specs()
        all_variants = generate_all_variants(specs)
        for vid in all_variants:
            assert "__v_" in vid, f"Variant ID '{vid}' missing __v_ separator"

    def test_no_empty_specs(self):
        from sregym.conductor.problems.variant_specs import get_all_variant_specs
        specs = get_all_variant_specs()
        for spec in specs:
            variants = generate_variants(spec)
            assert len(variants) > 0, f"Spec '{spec.base_name}' produced zero variants"

    def test_services_by_app_coverage(self):
        """Verify SERVICES_BY_APP has entries for the standard apps."""
        assert "social_network" in SERVICES_BY_APP
        assert "hotel_reservation" in SERVICES_BY_APP
        assert "astronomy_shop" in SERVICES_BY_APP
        for app, services in SERVICES_BY_APP.items():
            assert len(services) > 0, f"No services for {app}"
