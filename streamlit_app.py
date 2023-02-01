import os
from tempfile import NamedTemporaryFile

import pandas as pd
import streamlit as st

from fsspec_connection import FileConnection

df = pd.DataFrame({"a": [1, 2, 3], "b": [2, 3, 4]})

local, s3, s3_other, gcs, gcs_other = st.tabs(
    [
        "Local files",
        "S3 files",
        "S3 files (other credentials)",
        "GCS files",
        "GCS files (other credentials)",
    ]
)
with local:
    st.write("## Working with local files")
    with st.echo():
        conn = FileConnection()
        text_file = "test.txt"
        csv_file = "test.csv"
        parquet_file = "test.parquet"

        # Text files
        with conn.open(text_file, "wt") as f:
            f.write("This is a test")

        st.write(conn.read_text(text_file))

        # CSV Files
        with conn.open(csv_file, "wt") as f:
            df.to_csv(f, index=False)

        st.write(conn.read_csv(csv_file))

        st.write("## Parquet files")
        with conn.open(parquet_file, "wb") as f:
            df.to_parquet(f)

        st.write(conn.read_parquet(parquet_file))


with s3:
    st.write("## Working with S3 files")
    st.write("Credentials are stored in secrets.toml")

    st.code(
        """
# In secrets.toml
[connections.s3]
protocol = "s3"
key = "..."
secret = "..."
    """,
        language="toml",
    )

    with st.echo():
        conn = FileConnection("s3")  # protocol specified in secrets.toml

        text_file = "st-connection-test/test.txt"
        csv_file = "st-connection-test/test.csv"
        parquet_file = "st-connection-test/test.parquet"

        st.write("## Text files")

        try:
            st.write(conn.read_text(text_file))
        except FileNotFoundError:
            with conn.open(text_file, "wt") as f:
                f.write("This is a test")
            st.write(conn.read_text(text_file))

        st.write("## CSV Files")
        try:
            st.write(conn.read_csv(csv_file))
        except FileNotFoundError:
            with conn.open(csv_file, "wt") as f:
                df.to_csv(f, index=False)
            st.write(conn.read_csv(csv_file))

        st.write("## Parquet Files")
        try:
            st.write(conn.read_parquet(parquet_file))
        except FileNotFoundError:
            with conn.open(parquet_file, "wb") as f:
                df.to_parquet(f)
            st.write(conn.read_parquet(parquet_file))

with s3_other:
    st.write("## Working with S3 files")

    # HACK to get the environment variables set
    secrets = st.secrets["connections"]["s3"]

    os.environ["AWS_ACCESS_KEY_ID"] = secrets["key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = secrets["secret"]

    st.write(
        "Credentials stored in `~/.aws/config` or `AWS_ACCESS_KEY_ID` & "
        "`AWS_SECRET_ACCES_KEY` environment variables"
    )

    with st.echo():
        conn = FileConnection("s3-other", protocol="s3")

        text_file = "st-connection-test/test2.txt"
        csv_file = "st-connection-test/test2.csv"
        parquet_file = "st-connection-test/test2.parquet"

        st.write("## Text files")
        try:
            st.write(conn.read_text(text_file))
        except FileNotFoundError:
            with conn.open(text_file, "wt") as f:
                f.write("This is a test")
            st.write(conn.read_text(text_file))

        st.write("## CSV Files")
        try:
            st.write(conn.read_csv(csv_file))
        except FileNotFoundError:
            with conn.open(csv_file, "wt") as f:
                df.to_csv(f, index=False)
            st.write(conn.read_csv(csv_file))

        st.write("## Parquet Files")
        try:
            st.write(conn.read_parquet(parquet_file))
        except FileNotFoundError:
            with conn.open(parquet_file, "wb") as f:
                df.to_parquet(f)
            st.write(conn.read_parquet(parquet_file))


with gcs:
    st.write("## Working with Google Cloud Storage files")
    st.write("Credentials are set in secrets.toml")

    st.code(
        """
# In secrets.toml
[connections.gcs]
protocol = "gcs"
type = "..."
project_id = "..."
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n..."
client_email = "..."
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "..."
    """,
        language="toml",
    )

    with st.echo():
        conn = FileConnection("gcs")  # protocol specified in secrets.toml

        text_file = "st-connection-test/test.txt"
        csv_file = "st-connection-test/test.csv"
        parquet_file = "st-connection-test/test.parquet"

        st.write("## Text files")

        try:
            st.write(conn.read_text(text_file))
        except FileNotFoundError:
            with conn.open(text_file, "wt") as f:
                f.write("This is a test")
            st.write(conn.read_text(text_file))

        st.write("## CSV Files")
        try:
            st.write(conn.read_csv(csv_file))
        except FileNotFoundError:
            with conn.open(csv_file, "wt") as f:
                df.to_csv(f, index=False)
            st.write(conn.read_csv(csv_file))

        st.write("## Parquet Files")
        try:
            st.write(conn.read_parquet(parquet_file))
        except FileNotFoundError:
            with conn.open(parquet_file, "wb") as f:
                df.to_parquet(f)
            st.write(conn.read_parquet(parquet_file))

with gcs_other:
    "## Working with Google Cloud Storage files"
    st.write("Credentials are provided by a path to a service account json file")

    connection_details = dict(st.secrets["connections"]["gcs"])

    del connection_details["protocol"]

    with NamedTemporaryFile("w+", suffix=".json") as f:
        import json

        json.dump(connection_details, f)
        f.seek(0)

        credentials_file_name = f.name

        with st.echo():
            conn = FileConnection(
                "gcs-other", protocol="gcs", token=credentials_file_name
            )

            text_file = "st-connection-test/test4.txt"
            csv_file = "st-connection-test/test4.csv"
            parquet_file = "st-connection-test/test4.parquet"

            st.write("## Text files")
            try:
                st.write(conn.read_text(text_file))
            except FileNotFoundError:
                with conn.open(text_file, "wt") as f:
                    f.write("This is a test")
                st.write(conn.read_text(text_file))

            st.write("## CSV Files")
            try:
                st.write(conn.read_csv(csv_file))
            except FileNotFoundError:
                with conn.open(csv_file, "wt") as f:
                    df.to_csv(f, index=False)
                st.write(conn.read_csv(csv_file))

            st.write("## Parquet Files")
            try:
                st.write(conn.read_parquet(parquet_file))
            except FileNotFoundError:
                with conn.open(parquet_file, "wb") as f:
                    df.to_parquet(f)
                st.write(conn.read_parquet(parquet_file))
