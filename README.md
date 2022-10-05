# sqlakeyset: modified version using 2.0-style ORM queries and asyncio

This fork contains a heavily modified version of the [sqlakeyset](https://github.com/djrobstep/sqlakeyset) library which uses SQLAlchemy 2.0-style ORM queries and asyncio.

Notes:
1. This version of the library was written to work exclusively with 2.0 style over asyncio, so all 1.3/1.4 related code
was removed completely.
2. The interface for paging is different from the original interface. It now supports previous, first, last, next on paging.

## How to install
 - If you're using poetry, add the following block to your pyproject.toml file.

```
[tool.poetry.dependencies]
aio_sqlakeyset = {git = "https://github.com/virgodesigns/sqlakeyset", rev = "master"}
```

- If you're using pip, you can install using:

```
pip install git+https://github.com/virgodesigns/sqlakeyset@master
```
