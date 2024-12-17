from __future__ import annotations

from minmodkg.grade_tonnage_model import GradeTonnageEstimate, GradeTonnageModel
from minmodkg.misc import assert_not_none

MineralInventory = GradeTonnageModel.MineralInventory


class TestGradeTonnageModel:

    model = GradeTonnageModel()

    def test_resource_estimate(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=170.0,
            contained_metal=2.05,
        )

    def test_select_recent_data(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=70.0,
            contained_metal=1.05,
        )

    def test_material_form(self):
        assert GradeTonnageEstimate(
            tonnage=32.48,
            contained_metal=0.4872,
        ).approx_eq(
            assert_not_none(
                self.model(
                    [
                        MineralInventory(
                            id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                            date="2005-11",
                            zone=None,
                            category=["https://minmod.isi.edu/resource/Indicated"],
                            material_form_conversion=0.464,
                            ore_value=70.0,
                            ore_unit="https://minmod.isi.edu/resource/Q202",
                            grade_value=1.5,
                            grade_unit="https://minmod.isi.edu/resource/Q201",
                        ),
                    ],
                )
            ).total_estimate
        )

    def test_zero_grade_or_tonnage_estimate(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Indicated",
                            "https://minmod.isi.edu/resource/Inferred",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=0.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=100.0,
            contained_metal=0.0,
        )

        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Indicated",
                            "https://minmod.isi.edu/resource/Inferred",
                        ],
                        material_form_conversion=None,
                        ore_value=0.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=5.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=0.0,
            contained_metal=0.0,
        )

    def test_ignore_negative_grade_or_tonnage(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=-1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=100.0,
            contained_metal=1.0,
        )

        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=-1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=70.0,
            contained_metal=1.05,
        )

    def test_ignore_invalid_unit(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q201",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=70.0,
            contained_metal=1.05,
        )

    def test_ignore_invalid_category(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Indicated",
                            "https://minmod.isi.edu/resource/Probable",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=70.0,
            contained_metal=1.05,
        )

    def test_ignore_all_invalid_data(self):
        assert (
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Indicated",
                            "https://minmod.isi.edu/resource/Probable",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=-1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ) is None

    def test_handle_zone(self):
        # from different zones are summed up
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone="zone 1",
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone="zone 2",
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=170.0,
            contained_metal=2.05,
        )

        # but if the data for the overall site is also provided, we will compare
        # and return the larger estimate
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone="zone 1",
                        category=["https://minmod.isi.edu/resource/Indicated"],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone="zone 2",
                        category=["https://minmod.isi.edu/resource/Inferred"],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=["https://minmod.isi.edu/resource/Probable"],
                        material_form_conversion=None,
                        ore_value=170.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=170.0,
            contained_metal=2.55,
        )

    def test_duplicated_zone(self):
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Indicated",
                            "https://minmod.isi.edu/resource/Inferred",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2005-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Indicated",
                            "https://minmod.isi.edu/resource/Measured",
                        ],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=70.0,
            contained_metal=1.05,
        )

    def test_combine_extracted_data(self):
        """Combine the extracted data in the past into the grade/tonnage estimate."""
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2010-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Inferred",
                            "https://minmod.isi.edu/resource/Indicated",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Extracted",
                        ],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=170.0,
            contained_metal=2.05,
        )

    def test_combine_cumulative_extracted(self):
        """The cumulative extracted data has higher priority than the extracted data"""
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2010-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Inferred",
                            "https://minmod.isi.edu/resource/Indicated",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/CumulativeExtracted",
                        ],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Extracted",
                        ],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=170.0,
            contained_metal=1.7,
        )

    def test_combine_recent_cumulative_extracted(self):
        """We should only use the most recent cumulative extracted data"""
        assert assert_not_none(
            self.model(
                [
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2010-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Inferred",
                            "https://minmod.isi.edu/resource/Indicated",
                        ],
                        material_form_conversion=None,
                        ore_value=100.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2007-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/CumulativeExtracted",
                        ],
                        material_form_conversion=None,
                        ore_value=50.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/CumulativeExtracted",
                        ],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.0,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                    MineralInventory(
                        id="02c9593d2550e6bf75381aba60fd1ac24039518ac35726c3471ed94d02c666aa1b:1",
                        date="2006-11",
                        zone=None,
                        category=[
                            "https://minmod.isi.edu/resource/Extracted",
                        ],
                        material_form_conversion=None,
                        ore_value=70.0,
                        ore_unit="https://minmod.isi.edu/resource/Q202",
                        grade_value=1.5,
                        grade_unit="https://minmod.isi.edu/resource/Q201",
                    ),
                ],
            )
        ).total_estimate == GradeTonnageEstimate(
            tonnage=150.0,
            contained_metal=1.5,
        )


class TestGradeTonnageEstimate:

    def test_get_zero_grade_or_tonnage(self):
        assert (
            GradeTonnageEstimate(tonnage=100.0, contained_metal=0.0).get_grade() == 0.0
        )
        assert GradeTonnageEstimate(tonnage=0.0, contained_metal=0.0).get_grade() == 0.0
