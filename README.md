# sqlakeyset: modified version using 2.0-style ORM queries and asyncio

This fork contains a heavily modified version of the [sqlakeyset](https://github.com/djrobstep/sqlakeyset) library which uses SQLAlchemy 2.0-style ORM queries and asyncio.

Notes:
1. This version of the library was written to work exclusively with 2.0 style over asyncio, so all 1.3/1.4 related code
was removed completely.
2. The interface for paging is different from the original interface. It now supports previous, first, last, next on paging.

## How to build and publish
- First setup repository for poetry to push to. To do that, run the following command `poetry config repositories.gitlab https://gitlab.com/api/v4/projects/<project_id>/packages/pypi`.
- Now setup credentials for that repository. `poetry config http-basic.gitlab <access token name> <acess token>`
- Build the package by running `poetry build`
- Publish the package by running `poetry publish --repository gitlab`

## How to install
 - If you're using poetry, add the following block to your pyproject.toml file.

```
[[tool.poetry.source]]
name = "gitlab"
url = "https://gitlab.com/api/v4/projects/<project_id>/packages/pypi/simple"
```
and then run `poetry config http-basic.gitlab <access token name> <acess token>`
- If you're using pip, you can install using:
`pip install aio-sqlakeyset --no-deps --index-url https://<access token name>:<access token password>/api/v4/projects/<project_id>/packages/pypi/simple` or you can update similar config in global `pip.conf` file.

