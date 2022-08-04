from __future__ import annotations

import pathlib
import json

import sqlalchemy as sa
from sqlalchemy.orm import Session

import click
import pandas as pd

from . import models
from . import confluence


@click.group(context_settings={"show_default": True})
def main():
    pass


@main.command()
@click.option(
    "--products",
    help="location of mris",
    default="/corral-secure/projects/A2CPS/products/mris",
    type=click.Path(
        exists=True, file_okay=False, path_type=pathlib.Path, resolve_path=True
    ),
)
@click.option(
    "--url",
    default="phantom.db",
    type=click.Path(
        exists=False, writable=True, dir_okay=False, path_type=pathlib.Path
    ),
    help="url of database to write. see https://docs.sqlalchemy.org/en/14/dialects/sqlite.html",
)
def init(products: pathlib.Path, url: pathlib.Path = "phantom.db"):
    """write the database"""

    engine = sa.create_engine(f"sqlite:///{url}", future=True)
    models.Base.metadata.create_all(engine)

    with Session(engine) as session:
        for f in products.glob(("*/dicoms/*QC*zip")):
            session.add(models.DICOM.from_path(f))
        for f in products.glob(("*/bids/*QC*")):
            session.add(models.BIDS.from_path(f, session))
        for f in products.glob("*/bids/*QC*/sub*/ses*/anat/*T1w.json"):
            session.add(models.T1w.from_path(f, session))
        for f in products.glob("*/bids/*QC*/sub*/ses*/func/*bold.json"):
            session.add(models.BOLD.from_path(f, session))
        for f in products.glob("*/bids/*QC*/sub*/ses*/dwi/*dwi.json"):
            session.add(models.DWI.from_path(f, session))
        for qa in products.glob("*/aa-fmri-phantom-qa/*/*/*table.csv"):
            for row in pd.read_csv(qa).itertuples():
                session.add(
                    models.BOLDSlice(
                        bold=session.get(models.BOLD, qa.absolute().parents[0].stem),
                        slice=row.slice,
                        signal=row.signal,
                        signal_p2p=row.signal_p2p,
                        snr=row.snr,
                        ghost=row.ghost,
                    )
                )
        session.commit()


def _export_table(
    table: str, url: pathlib.Path = "phantom.db", out: pathlib.Path | None = None
) -> pd.DataFrame:
    engine = sa.create_engine(f"sqlite:///{url}", future=True)

    with engine.connect() as conn:
        d = pd.read_sql_table(table, con=conn)

    if out:
        d.to_csv(out, index=False, sep="\t")

    return d


@main.command()
@click.argument("table", default="log")
@click.option(
    "--out",
    default="phantom-log.tsv",
    help="csv file to write out",
    type=click.Path(
        exists=False, writable=True, dir_okay=False, path_type=pathlib.Path
    ),
)
@click.option(
    "--url",
    default="phantom.db",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    help="filename of database to write",
)
def export_table(table, url, out):
    """read table from url--mainly for testing"""
    return _export_table(table=table, url=url, out=out)


@main.command()
@click.option(
    "--out",
    default=None,
    help="csv file to write out",
    type=click.Path(exists=False, writable=True, path_type=pathlib.Path),
)
@click.option(
    "--url",
    default="phantom.db",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    help="filename of database to write",
)
@click.option(
    "--post",
    is_flag=True,
    show_default=True,
    default=False,
    help="Whether to upload to Confluence",
)
@click.option(
    "--secrets",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default="secrets.json",
    help="name of json file containing PAT",
)
def write_and_post(
    out: pathlib.Path | None = None,
    post: bool = False,
    url: pathlib.Path = "phantom.db",
    secrets: pathlib.Path = pathlib.Path("secrets.json"),
) -> str:

    oldlog = _export_table(table="log", url=url)
    log = confluence.Log.from_token(token=json.loads(secrets.read_text()).get("PAT"))
    newlog = log.merge_logs(oldlog)

    if post:
        log.post_log(newlog)
    if out:
        newlog.to_csv(out, sep="\t", index=False)

    return out
