from __future__ import annotations

import attrs
import requests
import pandas as pd
from bs4 import BeautifulSoup

from atlassian import Confluence


@attrs.define()
class Log:

    # https://confluence.a2cps.org/display/DOC/Phantom+Log
    confluence: Confluence
    oldlog: pd.DataFrame
    soup: BeautifulSoup
    pageid: str
    pagetitle: str = "Phantom Log"

    @classmethod
    def from_token(
        cls,
        token: str,
        site: str = "https://confluence.a2cps.org/",
        pem: str = "confluence-a2cps-org-chain.pem",
        pageid: str = "44237591",
    ) -> Log:
        # initalize session
        s = requests.Session()
        s.headers.update({"Authorization": f"Bearer {token}"})
        confluence = Confluence(url=site, session=s, verify_ssl=pem)

        # get oldlog
        page = confluence.get_page_by_id(pageid, expand="body.storage")
        page_content = page.get("body").get("storage").get("value")
        table = pd.read_html(page_content)
        soup = BeautifulSoup(page_content, "html.parser")
        ##  Only one table on the page
        oldlog = table[0][["id", "notes"]]

        return cls(
            confluence=confluence,
            oldlog=oldlog,
            pageid=pageid,
            soup=soup,
        )

    def merge_logs(self, oldlog: pd.DataFrame) -> pd.DataFrame:
        # processing serves as base because that builds the most complete list of scans, a list
        # that is based on the files found in /products/<site>/dicoms
        newlog = oldlog.merge(self.oldlog, on=["id"], how="left")
        return (
            newlog[
                [
                    "site",
                    "date",
                    "notes",
                    "dicom",
                    "bids",
                    "bids_validation",
                    "T1w",
                    "b1000",
                    "b2000",
                    "bold",
                    "id",
                ]
            ]
            .sort_values(by=["site", "date"])
            .fillna("")
        )

    def post_log(self, newlog: pd.DataFrame) -> None:
        date_columns = newlog.select_dtypes(include=['datetime']).columns
        newlog[date_columns] = newlog[date_columns].apply(lambda x: x.dt.strftime("%y-%m-%d")).fillna('')

        self.soup.table.replace_with(
            BeautifulSoup(newlog.to_html(index=False, na_rep=""), "html.parser")
        )
        # update page with new content
        self.confluence.update_page(
            page_id=self.pageid, title=self.pagetitle, body=str(self.soup)
        )
