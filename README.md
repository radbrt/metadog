# Metadog

The new storebrand open-source, headless observability tool.

## Our goals
Easily scan data sources like databases, SFTP servers and cloud storage through a declarative yaml configuration

- Keep an inventory of the data sources, and track changes
- Scan and profile the data
- Automatically monitor for anomalies
- Store and track the results in a postgres or sqlite database
- Enable other tools to use the data


## What it is not
Metadog is not a dashboard for your data. Metadog is completely headless, but is designed to provide read-access to other tools. Make your own dashboard, if you want one.

Metadog does not have an API. We believe it is both safer and more useful for data teams to have direct database access to a sensible data model rather than yet another REST API to deal with.

## Get started

Initialize a project with `metadog init <name-of-project>`. This will create a new folder with the base files you need to get started.

In the `metadog.yaml` file, add the data sources you want.
	- For databases: If you want to monitor table statistics, add `analyze: True`.

Choose the backend database you want to use by updating the `METADOG_BACKEND_URI` environment value in the `.env` file, or set it as a system environment variable. By default, Metadog uses a local SQLite database named `metadog.db`. The database will be created the first time metadog runs.

Check that the configuration is valid by running `metadog validate`.

Once the validation passes, you can run a scan with `metadog scan`. You can limit the scan to only a subset of sources using the `-s` or `--select` flag, and turn off table statistics at runtime with the `--no-stats` flag.

## Q&A

**Q**: Why doesn't metadog use a standard data model like <insert-your-favorite-metadata-standard>?
**A**: Some possible reasons include:
- It was way too complex, it would take too much for users to understand it
- It didn't contain some fields I wanted
- I didn't know about it


