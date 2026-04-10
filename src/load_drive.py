"""Upload a DataFrame as a CSV to Google Drive via the Drive API.

Uses OAuth 2.0 user credentials (InstalledAppFlow) so files land in the
authorizing user's own Drive, where a personal Gmail account has storage
quota. Service accounts cannot own files in a personal My Drive, which
is why the previous implementation hit a quota wall.

Files are updated in-place if a file with the same name already exists
in the target folder, so repeated pipeline runs do not pile up
duplicates inside Drive.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


# drive.file is the narrowest scope that still lets the app create and
# update files it owns inside the target folder.
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def _get_credentials(client_secret_path: Path, token_path: Path) -> Credentials:
    """Return valid OAuth user credentials, refreshing or prompting as needed.

    On first run a browser window is opened for user consent and the
    resulting refresh token is persisted to ``token_path`` so subsequent
    runs (including inside Airflow containers without a browser) can
    reuse it non-interactively.
    """
    creds: Credentials | None = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            str(client_secret_path), SCOPES
        )
        creds = flow.run_local_server(port=0)

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json())
    return creds


def _find_file_id(service, filename: str, folder_id: str) -> str | None:
    """Return the Drive file ID if a file with this name exists, else None.

    The query language escapes single quotes with a backslash, so any
    apostrophe in the filename has to be doubled up before interpolation.
    """
    safe_name = filename.replace("\\", "\\\\").replace("'", "\\'")
    query = (
        f"name = '{safe_name}' "
        f"and '{folder_id}' in parents "
        "and trashed = false"
    )
    results = (
        service.files()
        .list(q=query, fields="files(id, name)", pageSize=1)
        .execute()
    )
    files = results.get("files", [])
    return files[0]["id"] if files else None


def upload_csv_to_drive(
    df: pd.DataFrame,
    filename: str,
    folder_id: str,
    client_secret_path: Path,
    token_path: Path,
) -> None:
    """Save a DataFrame as CSV and upload it to Google Drive.

    If a file with the same name already exists in ``folder_id`` it is
    updated in place; otherwise a new file is created.
    """
    creds = _get_credentials(client_secret_path, token_path)
    service = build("drive", "v3", credentials=creds)

    tmp_path = Path("/tmp") / filename
    df.to_csv(tmp_path, index=False)

    try:
        media = MediaFileUpload(str(tmp_path), resumable=True)
        existing_id = _find_file_id(service, filename, folder_id)

        if existing_id:
            service.files().update(
                fileId=existing_id, media_body=media, fields="id"
            ).execute()
        else:
            service.files().create(
                body={"name": filename, "parents": [folder_id]},
                media_body=media,
                fields="id",
            ).execute()
    finally:
        tmp_path.unlink(missing_ok=True)
