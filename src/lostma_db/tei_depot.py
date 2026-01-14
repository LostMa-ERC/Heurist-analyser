from dataclasses import dataclass
from pathlib import Path
import re
import requests


@dataclass(frozen=True)
class TeiDepotClient:
    owner: str = "LostMa-ERC"
    repo: str = "tei-depot"
    branch: str = "main"
    texts_dir: str = "texts"
    timeout: int = 30

    @property
    def api_base(self) -> str:
        return "https://api.github.com"

    def _get_json(self, url: str) -> dict:
        r = requests.get(url, timeout=self.timeout, headers={"Accept": "application/vnd.github+json"})
        r.raise_for_status()
        return r.json()

    def list_xml_paths(self) -> list[str]:
        """
        List all XML files on https://github.com/LostMa-ERC/tei-depot
        """
        # Use GitHub API with git/trees?recursive=1.
        ref = self._get_json(f"{self.api_base}/repos/{self.owner}/{self.repo}/git/ref/heads/{self.branch}")
        commit_sha = ref["object"]["sha"]
        commit = self._get_json(f"{self.api_base}/repos/{self.owner}/{self.repo}/git/commits/{commit_sha}")
        tree_sha = commit["tree"]["sha"]
        tree = self._get_json(f"{self.api_base}/repos/{self.owner}/{self.repo}/git/trees/{tree_sha}?recursive=1")
        prefix = self.texts_dir.rstrip("/") + "/"
        xml_paths = [
            item["path"]
            for item in tree.get("tree", [])
            if item.get("type") == "blob"
            and isinstance(item.get("path"), str)
            and item["path"].startswith(prefix)
            and item["path"].lower().endswith(".xml")
        ]
        return sorted(xml_paths)

    def build_tei_index(self, xml_paths: list[str] | None = None) -> dict[str, str]:
        """
        Build index {id: path} from files like texts/**/text_<id>.xml
        """
        if xml_paths is None:
            xml_paths = self.list_xml_paths()
        pattern = re.compile(rf"^{re.escape(self.texts_dir)}/.+/text_(.+)\.xml$", re.IGNORECASE)
        index: dict[str, str] = {}
        for fp in xml_paths:
            m = pattern.match(fp)
            if not m:
                continue
            tei_id = m.group(1)
            if tei_id in index and index[tei_id] != fp:
                raise ValueError(f"TEI ID collision: {tei_id} → {index[tei_id]} vs {fp}")
            index[tei_id] = fp
        return index

    def raw_url(self, path: str) -> str:
        return f"https://raw.githubusercontent.com/{self.owner}/{self.repo}/{self.branch}/{path}"

    def download_file(self, path: str, dest: Path) -> Path:
        dest = Path(dest)
        if dest.is_dir() or str(dest).endswith("/"):
            dest.mkdir(parents=True, exist_ok=True)
            out_path = dest / Path(path).name
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            out_path = dest
        url = self.raw_url(path)
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        out_path.write_bytes(r.content)
        return out_path

    def find_path_by_id(self, tei_id: str, index: dict[str, str] | None = None) -> str:
        tei_id = str(tei_id).strip()
        if index is None:
            index = self.build_tei_index()
        try:
            return index[tei_id]
        except KeyError:
            raise FileNotFoundError(f"No TEI file for id={tei_id!r}")

    def download_by_id(
        self,
        tei_id: str,
        dest_dir: str | Path,
        index: dict[str, str] | None = None
    ) -> Path:
        """
        Download the TEI file corresponding to the id
        """
        path = self.find_path_by_id(tei_id, index=index)
        return self.download_file(path, dest_dir)
