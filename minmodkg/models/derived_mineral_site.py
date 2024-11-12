from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Annotated, Any, Optional

import shapely.wkt
from fastapi import HTTPException
from minmodkg.config import MNR_NS, NS_MNO
from minmodkg.grade_tonnage_model import GradeTonnageModel, SiteGradeTonnage
from minmodkg.misc.geo import reproject_wkt
from minmodkg.misc.sparql import Triples
from minmodkg.models.crs import CRS
from minmodkg.models.material_form import MaterialForm
from minmodkg.models.mineral_site import MineralSite, norm_literal, norm_uri
from minmodkg.typing import IRI, InternalID, Triple
from pydantic import BaseModel, Field
from rdflib import OWL, RDFS, SKOS, Graph
from rdflib.term import Node


class GradeTonnage(BaseModel):
    commodity: InternalID
    total_contained_metal: Optional[float] = None
    total_tonnage: Optional[float] = None
    total_grade: Optional[float] = None

    @staticmethod
    def from_graph(id: Node, g: Graph):
        commodity = norm_uri(next(g.objects(id, NS_MNO.commodity)))
        assert commodity.startswith(MNR_NS), commodity
        commodity = commodity[len(MNR_NS) :]
        return GradeTonnage(
            commodity=commodity,
            total_contained_metal=norm_literal(
                next(g.objects(id, NS_MNO.total_contained_metal), None)
            ),
            total_tonnage=norm_literal(next(g.objects(id, NS_MNO.total_tonnage), None)),
            total_grade=norm_literal(next(g.objects(id, NS_MNO.total_grade), None)),
        )


class Coordinates(BaseModel):
    lat: Annotated[float, "Latitude"]
    lon: Annotated[float, "Longitude"]


class DerivedMineralSite(BaseModel):
    uri: IRI
    coordinates: Optional[Coordinates]
    grade_tonnage: list[GradeTonnage]

    @staticmethod
    def from_mineral_site(
        site: MineralSite,
        material_form: dict[str, MaterialForm],
        crss: dict[str, CRS],
    ):
        if site.location_info is not None and site.location_info.location is not None:
            if site.location_info.crs is not None:
                if site.location_info.crs.normalized_uri is None:
                    crs = "EPSG:4326"
                else:
                    crs = crss[site.location_info.crs.normalized_uri].name

            geometry = shapely.wkt.loads(site.location_info.location)
            centroid = shapely.wkt.dumps(shapely.centroid(geometry))
            centroid = reproject_wkt(centroid, crs, "EPSG:4326")

            m = re.match(
                r"POINT \(([+-]?(?:[0-9]*[.])?[0-9]+) ([+-]?(?:[0-9]*[.])?[0-9]+)\)",
                centroid,
            )
            assert m is not None, centroid
            coordinates = Coordinates(
                lat=float(m.group(2)),
                lon=float(m.group(1)),
            )
        else:
            coordinates = None

        invs: dict[str, list] = defaultdict(list)
        grade_tonnage_model = GradeTonnageModel()
        commodities = set()

        for inv_id, inv in enumerate(site.mineral_inventory):
            if inv.commodity.normalized_uri is None:
                continue

            commodity = inv.commodity.normalized_uri
            commodities.add(commodity)

            if (
                inv.ore is None
                or inv.ore.value is None
                or inv.ore.unit is None
                or inv.ore.unit.normalized_uri is None
                or inv.grade is None
                or inv.grade.value is None
                or inv.grade.unit is None
                or inv.grade.unit.normalized_uri is None
                or len(inv.category) == 0
            ):
                continue

            mi_form_conversion = None
            if (
                inv.material_form is not None
                and inv.material_form.normalized_uri is not None
            ):
                mi_form_conversion = material_form[
                    inv.material_form.normalized_uri
                ].conversion

            invs[commodity].append(
                GradeTonnageModel.MineralInventory(
                    id=str(inv_id),
                    date=inv.date,
                    zone=inv.zone,
                    category=[
                        cat.normalized_uri
                        for cat in inv.category
                        if cat.normalized_uri is not None
                    ],
                    material_form_conversion=mi_form_conversion,
                    ore_value=inv.ore.value,
                    ore_unit=inv.ore.unit.normalized_uri,
                    grade_value=inv.grade.value,
                    grade_unit=inv.grade.unit.normalized_uri,
                )
            )

        site_comms = []
        for commodity, gt_invs in invs.items():
            grade_tonnage = grade_tonnage_model(gt_invs) or SiteGradeTonnage()
            if grade_tonnage.total_reserve_tonnage is not None and (
                grade_tonnage.total_resource_tonnage is None
                or grade_tonnage.total_reserve_tonnage
                > grade_tonnage.total_resource_tonnage
            ):
                total_grade = grade_tonnage.get_total_reserve_grade()
                total_tonnage = grade_tonnage.total_reserve_tonnage
                total_contained_metal = grade_tonnage.total_reserve_contained_metal
            else:
                total_grade = grade_tonnage.get_total_resource_grade()
                total_tonnage = grade_tonnage.total_resource_tonnage
                total_contained_metal = grade_tonnage.total_resource_contained_metal

            site_comms.append(
                GradeTonnage(
                    commodity=commodity,
                    total_contained_metal=total_contained_metal,
                    total_tonnage=total_tonnage,
                    total_grade=total_grade,
                )
            )
        for comm in commodities:
            if comm not in invs:
                site_comms.append(GradeTonnage(commodity=comm))

        return DerivedMineralSite(
            uri=site.uri,
            coordinates=coordinates,
            grade_tonnage=site_comms,
        )

    def get_shorten_triples(self) -> list[Triple]:
        """Get triples shorten with the following prefixes:

        `:`: MNO_NS
        rdf: RDF
        rdfs: RDFS
        mnr: MNR_NS
        owl: OWL
        """
        assert self.uri.startswith(MNR_NS), self.uri
        site_id = f"mnr:{self.uri[len(MNR_NS) :]}"
        triples = []

        if self.coordinates is not None:
            triples.append(
                (
                    site_id,
                    ":lat",
                    str(self.coordinates.lat),
                )
            )
            triples.append(
                (
                    site_id,
                    ":lon",
                    str(self.coordinates.lon),
                )
            )

        gtnode_id_prefix = f"mnr:{site_id}__gt__"
        for gt in self.grade_tonnage:
            gtnode_id = gtnode_id_prefix + gt.commodity
            triples.append((gtnode_id, ":commodity", f"mnr:{gt.commodity}"))
            if gt.total_contained_metal is not None:
                triples.append(
                    (
                        gtnode_id,
                        ":total_contained_metal",
                        str(gt.total_contained_metal),
                    )
                )
                triples.append(
                    (
                        gtnode_id,
                        ":total_tonnage",
                        str(gt.total_tonnage),
                    )
                )
                triples.append(
                    (
                        gtnode_id,
                        ":total_grade",
                        str(gt.total_grade),
                    )
                )

        return triples
