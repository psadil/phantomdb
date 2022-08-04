from __future__ import annotations

from dataclasses import dataclass, field
import datetime
import json
import pathlib
import re
import zipfile

import sqlalchemy as sa
from sqlalchemy.orm import registry, relationship, backref, Session

import pydicom

from . import views


def _get_path_creationtime(f: pathlib.Path) -> datetime.date:
    return datetime.datetime.fromtimestamp(f.stat().st_ctime).date()


mapper_registry = registry()


@mapper_registry.mapped
@dataclass
class DICOM:
    __tablename__ = "dicoms"

    __sa_dataclass_metadata_key__ = "sa"
    id: str = field(metadata={"sa": sa.Column(sa.Text, primary_key=True)})
    site: str = field(metadata={"sa": sa.Column(sa.Text)})
    day: datetime.date = field(metadata={"sa": sa.Column(sa.Date)})
    acquisition_day: datetime.date | None = field(
        default=None, metadata={"sa": sa.Column(sa.Date)}
    )
    t1w: list[T1w] = field(
        default_factory=list,
        metadata={"sa": relationship("T1w", back_populates="dicom")},
    )
    bold: list[BOLD] = field(
        default_factory=list,
        metadata={"sa": relationship("BOLD", back_populates="dicom")},
    )

    dwi: list[DWI] = field(
        default_factory=list,
        metadata={"sa": relationship("DWI", back_populates="dicom")},
    )

    @classmethod
    def from_path(cls, f: pathlib.Path) -> DICOM:
        return cls(
            id=f.stem,
            day=_get_path_creationtime(f),
            acquisition_day=cls._extract_phantom_date(f),
            site=cls._extract_site(f),
        )

    @staticmethod
    def _extract_site(filename: pathlib.Path) -> str:
        return (
            re.search(r"(ns|ws|sh|ui|uc|um)", str(filename.stem), flags=re.IGNORECASE)
            .group(0)
            .upper()
        )

    @staticmethod
    def _extract_phantom_date(filename: pathlib.Path) -> datetime.date | None:
        if not zipfile.is_zipfile(filename):
            return
        site_zip = zipfile.ZipFile(filename)
        for listing in site_zip.filelist:
            if not listing.is_dir() and not "DICOMDIR" in listing.filename:
                with site_zip.open(listing) as dcm:
                    header = pydicom.dcmread(dcm, stop_before_pixels=True)
                break

        day = datetime.datetime.strptime(header.get("AcquisitionDate"), "%Y%m%d").date()
        return day


@dataclass
class ScanMixin:
    __sa_dataclass_metadata_key__ = "sa"
    id: str = field(metadata={"sa": sa.Column(sa.Text, primary_key=True)})
    meta: dict = field(metadata={"sa": sa.Column(sa.JSON)})
    dicom_id: str | None = field(
        default=None, metadata={"sa": lambda: sa.Column(sa.ForeignKey("dicoms.id"))}
    )

    @classmethod
    def from_path(cls, f: pathlib.Path, session: Session) -> ScanMixin:
        return cls(
            id=f.stem,
            meta=json.loads(f.read_text()),
            dicom=session.get(DICOM, f.absolute().parents[3].stem),
        )


@mapper_registry.mapped
@dataclass
class T1w(ScanMixin):
    __tablename__ = "t1ws"

    dicom: DICOM | None = field(
        default=None,
        metadata={"sa": relationship("DICOM", back_populates="t1w")},
    )


@mapper_registry.mapped
@dataclass
class DWI(ScanMixin):
    __tablename__ = "dwis"

    dicom: DICOM | None = field(
        default=None,
        metadata={"sa": relationship("DICOM", back_populates="dwi")},
    )


@mapper_registry.mapped
@dataclass
class BOLD(ScanMixin):
    __tablename__ = "bolds"

    dicom: DICOM | None = field(
        default=None,
        metadata={"sa": relationship("DICOM", back_populates="bold")},
    )
    boldslice: list[BOLDSlice] = field(
        default_factory=list,
        metadata={"sa": relationship("BOLDSlice", back_populates="bold")},
    )


@mapper_registry.mapped
@dataclass
class BOLDSlice:
    __tablename__ = "boldslices"

    __sa_dataclass_metadata_key__ = "sa"
    id: int = field(
        init=False, metadata={"sa": sa.Column(sa.Integer, primary_key=True)}
    )
    slice: int | None = field(metadata={"sa": sa.Column(sa.Integer())})
    signal: float | None = field(metadata={"sa": sa.Column(sa.Float())})
    signal_p2p: float | None = field(metadata={"sa": sa.Column(sa.Float())})
    snr: float | None = field(metadata={"sa": sa.Column(sa.Float())})
    ghost: float | None = field(metadata={"sa": sa.Column(sa.Float())})
    bold_id: str | None = field(
        default=None, metadata={"sa": sa.Column(sa.ForeignKey("bolds.id"))}
    )
    bold: BOLD | None = field(
        default=None,
        metadata={"sa": relationship("BOLD", back_populates="boldslice")},
    )


@mapper_registry.mapped
@dataclass
class BIDS:
    __tablename__ = "bids"

    __sa_dataclass_metadata_key__ = "sa"
    id: int = field(
        init=False, metadata={"sa": sa.Column(sa.Integer, primary_key=True)}
    )
    dicom_id: str | None = field(
        default=None, metadata={"sa": sa.Column(sa.ForeignKey("dicoms.id"))}
    )
    dicom: DICOM | None = field(
        default=None,
        metadata={
            "sa": relationship("DICOM", backref=backref("session", uselist=False))
        },
    )
    day: datetime.date | None = field(default=None, metadata={"sa": sa.Column(sa.Date)})
    valid: bool | None = field(default=None, metadata={"sa": sa.Column(sa.Boolean)})

    @classmethod
    def from_path(cls, f: pathlib.Path, session: Session) -> BIDS:
        validation_src = pathlib.Path(str(f).replace("bids", "bids_validation"))
        if not validation_src.exists():
            valid = None
        elif len([x for x in validation_src.glob("*out")]) > 0:
            valid = True
        else:
            valid = False
        return cls(
            valid=valid,
            day=_get_path_creationtime(f),
            dicom=session.get(DICOM, f.stem),
        )


@mapper_registry.mapped
@dataclass
class B1000View:
    """DWI views exist mainly as helpers during creation of LogView"""

    __table__ = views.view(
        "b1000",
        mapper_registry.metadata,
        sa.select(
            DWI.id,
            DWI.dicom_id,
            sa.case(
                (DWI.id.like("%b1000%"), "Y"),
                else_="N",
            ).label("b1000"),
        ).where(DWI.id.like("%b1000%")),
    )


@mapper_registry.mapped
@dataclass
class B2000View:
    __table__ = views.view(
        "b2000",
        mapper_registry.metadata,
        sa.select(
            DWI.id,
            DWI.dicom_id,
            sa.case(
                (DWI.id.like("%b2000%"), "Y"),
                else_="N",
            ).label("b2000"),
        ).where(DWI.id.like("%b2000%")),
    )


# this view
@mapper_registry.mapped
@dataclass
class LogView:
    """
    This is the table that will be uploaded to Confluence, excluding a column for notes,
    which is pulled during the upload process
    """

    __table__ = views.view(
        "log",
        mapper_registry.metadata,
        sa.select(
            DICOM.site,
            DICOM.acquisition_day.label("date"),
            DICOM.day.label("dicom"),
            BIDS.day.label("bids"),
            sa.case(
                (BIDS.valid == 1, "Y"),
                (BIDS.valid == 0, "N"),
                else_="",
            ).label("bids_validation"),
            sa.case(
                ((BIDS.valid == None) | (BIDS.valid == 0), ""),
                ((BIDS.valid == 1) & (T1w.id != None), "Y"),
                else_="N",
            ).label("T1w"),
            sa.case(
                ((BIDS.valid == None) | (BIDS.valid == 0), ""),
                ((BIDS.valid == 1) & (B1000View.b1000 != None), "Y"),
                else_="N",
            ).label("b1000"),
            sa.case(
                ((BIDS.valid == None) | (BIDS.valid == 0), ""),
                ((BIDS.valid == 1) & (B2000View.b2000 != None), "Y"),
                else_="N",
            ).label("b2000"),
            sa.case(
                ((BIDS.valid == None) | (BIDS.valid == 0), ""),
                ((BIDS.valid == 1) & (BOLD.id != None), "Y"),
                else_="N",
            ).label("bold"),
            DICOM.id,
        )
        .join(
            BIDS, isouter=True
        )  # scan joins could be inner, but outer will help detect lingering products
        .join(T1w, isouter=True)
        .join(BOLD, isouter=True)
        .join(B1000View, isouter=True)
        .join(B2000View, isouter=True)
        .order_by(DICOM.site, DICOM.day),
    )


Base = mapper_registry.generate_base()


# import pandas as pd

# engine = sa.create_engine(f"sqlite:///database.db", future=True)
# Base.metadata.create_all(engine)

# with engine.connect() as con:
#     print(pd.read_sql_table("log", con=con))

# with engine.connect() as con:
#     b1000 = sa.select(
#         DWI.dicom_id,
#         sa.case(
#             (DWI.id.like("%b1000%"), "Y"),
#             else_="N",
#         ).label("b1000"),
#     ).where(DWI.id.like("%b1000%"))
#     b2000 = (
#         sa.select(
#             DWI.dicom_id,
#             sa.case(
#                 (DWI.id.like("%b2000%"), "Y"),
#                 else_="N",
#             ).label("b2000"),
#         )
#         .where(DWI.id.like("%b2000%"))
#         .subquery("b2")
#     )

#     oldlog = pd.read_sql_query(
#         sa.select(
#             DICOM.site,
#             DICOM.acquisition_day.label("date"),
#             DICOM.day.label("dicom"),
#             BIDS.day.label("bids"),
#             sa.case(
#                 (BIDS.valid == 1, "Y"),
#                 (BIDS.valid == 0, "N"),
#                 else_="",
#             ).label("bids_validation"),
#             sa.case(
#                 ((BIDS.valid == None) | (BIDS.valid == 0), ""),
#                 ((BIDS.valid == 1) & (T1w.id != None), "Y"),
#                 else_="N",
#             ).label("T1w"),
#             sa.case(
#                 ((BIDS.valid == None) | (BIDS.valid == 0), ""),
#                 ((BIDS.valid == 1) & (b1000.c.b1000 != None), "Y"),
#                 else_="N",
#             ).label("b1000"),
#             sa.case(
#                 ((BIDS.valid == None) | (BIDS.valid == 0), ""),
#                 ((BIDS.valid == 1) & (b2000.c.b2000 != None), "Y"),
#                 else_="N",
#             ).label("b2000"),
#             sa.case(
#                 ((BIDS.valid == None) | (BIDS.valid == 0), ""),
#                 ((BIDS.valid == 1) & (Func.id != None), "Y"),
#                 else_="N",
#             ).label("bold"),
#             DICOM.id,
#         )
#         .join(BIDS)
#         .join(T1w, isouter=True)
#         .join(Func, isouter=True)
#         .join(b1000, isouter=True)
#         .join(b2000, isouter=True)
#         .order_by(DICOM.site, DICOM.day),
#         con=con,
#     )
